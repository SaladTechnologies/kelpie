import {
  S3Client,
  GetObjectCommand,
  ListObjectsV2Command,
  ListObjectsV2CommandInput,
  DeleteObjectCommand,
} from "@aws-sdk/client-s3";
import { Progress, Upload } from "@aws-sdk/lib-storage";
import fs from "fs";
import { Readable } from "stream";
import path from "path";
import fsPromises from "fs/promises";
import { createGzip, createGunzip } from "zlib";
import { log } from "./logger";

const { AWS_REGION, AWS_DEFAULT_REGION } = process.env;

const s3Client = new S3Client({ region: AWS_REGION || AWS_DEFAULT_REGION });

function getDataRatioString(
  loaded: number | undefined,
  total: number | undefined
): string {
  let sizeString = "(??? B)";
  if (loaded && total && total > 1024 * 1024 * 1024) {
    const totalGB = (total / (1024 * 1024 * 1024)).toFixed(2);
    const progressGB = (loaded / (1024 * 1024 * 1024)).toFixed(2);
    sizeString = `(${progressGB}/${totalGB} GB)`;
  } else if (loaded && total && total > 1024 * 1024) {
    const totalMB = (total / (1024 * 1024)).toFixed(2);
    const progressMB = (loaded / (1024 * 1024)).toFixed(2);
    sizeString = `(${progressMB}/${totalMB} MB)`;
  } else if (loaded && total && total > 1024) {
    const totalKB = (total / 1024).toFixed(2);
    const progressKB = (loaded / 1024).toFixed(2);
    sizeString = `(${progressKB}/${totalKB} KB)`;
  } else if (loaded && total) {
    sizeString = `(${loaded}/${total} B)`;
  } else if (loaded && loaded > 1024 * 1024 * 1024) {
    const progressGB = (loaded / (1024 * 1024 * 1024)).toFixed(2);
    sizeString = `(${progressGB} GB)`;
  } else if (loaded && loaded > 1024 * 1024) {
    const progressMB = (loaded / (1024 * 1024)).toFixed(2);
    sizeString = `(${progressMB} MB)`;
  } else if (loaded && loaded > 1024) {
    const progressKB = (loaded / 1024).toFixed(2);
    sizeString = `(${progressKB} KB)`;
  } else if (loaded) {
    sizeString = `(${loaded} B)`;
  }
  return sizeString;
}

const ongoingUploads: Set<string> = new Set();

export async function uploadFile(
  localFilePath: string,
  bucketName: string,
  key: string,
  compress: boolean = false
): Promise<void> {
  try {
    if (ongoingUploads.has(localFilePath)) {
      log.info(`Upload of ${localFilePath} already in progress`);
      return;
    }
    ongoingUploads.add(localFilePath);
    log.info(`Uploading ${localFilePath} to s3://${bucketName}/${key}`);
    // Create a stream from the local file
    const fileStream = fs.createReadStream(localFilePath);
    let stream;
    if (compress) {
      log.debug(`Compressing ${localFilePath} file with gzip`);
      const gzipStream = createGzip();
      fileStream.pipe(gzipStream);
      stream = gzipStream;
      key = key + ".gz";
    } else {
      stream = fileStream;
    }

    // Set up the upload parameters
    const uploadParams = {
      Bucket: bucketName,
      Key: key,
      Body: stream,
    };

    // Perform the upload
    const parallelUploads3 = new Upload({
      client: s3Client,
      params: uploadParams,
      queueSize: 8,
      partSize: 10 * 1024 * 1024,
    });

    // Track progress
    parallelUploads3.on("httpUploadProgress", (progress: Progress) => {
      let sizeString = getDataRatioString(progress.loaded, progress.total);
      if (!sizeString) {
        log.warn({ progress }, "Progress data ratio string is empty");
      }
      let loadRatio = progress.loaded! / progress.total!;
      let percentString = "Calculating Total...";
      if (!isNaN(loadRatio)) {
        percentString = (loadRatio * 100).toFixed(2) + "%";
      }
      log.info(`Uploading ${key}: ${percentString} ${sizeString}`);
    });

    // Wait for the upload to finish
    await parallelUploads3.done();
    log.info("Upload completed successfully");
  } catch (err) {
    log.error("Error uploading file: ", err);
  }
  ongoingUploads.delete(localFilePath);
}

export async function downloadFile(
  bucketName: string,
  key: string,
  localFilePath: string,
  decompress: boolean = false
): Promise<void> {
  try {
    const start = Date.now();

    const isGzipped = decompress && key.endsWith(".gz");
    if (isGzipped) {
      localFilePath = localFilePath.replace(/\.gz$/, "");
    }
    // Set up the download parameters
    const downloadParams = {
      Bucket: bucketName,
      Key: key,
    };

    // Perform the download
    const data = await s3Client.send(new GetObjectCommand(downloadParams));

    return new Promise((resolve, reject) => {
      if (data.Body instanceof Readable) {
        let stream: Readable = data.Body;
        if (isGzipped) {
          log.debug(`Decompressing ${key} file with gunzip`);
          stream = stream.pipe(createGunzip());
        }

        // Loop through body chunks and write to file
        const writeStream = fs.createWriteStream(localFilePath);
        stream
          .pipe(writeStream)
          .on("error", (err: any) => reject(err))
          .on("close", () => {
            const end = Date.now();

            // Build a duration string that uses at most 2 significant figures (e.g. 1.2s, 1.23s, 1.23m)
            let durString = "";
            let durMs = end - start;
            if (durMs < 1000) {
              durString = `${durMs}ms`;
            } else if (durMs < 60000) {
              durString = `${(durMs / 1000).toFixed(2)}s`;
            } else {
              durString = `${(durMs / 60000).toFixed(2)}m`;
            }
            log.info(`${localFilePath} downloaded in ${durString}`);
            resolve();
          });
      }
    });
  } catch (err: any) {
    log.error("Error downloading file: ", err);
    throw err;
  }
}

async function listAllS3Objects(
  bucketName: string,
  prefix?: string
): Promise<string[]> {
  let continuationToken: string | undefined = undefined;
  const allKeys: string[] = [];

  do {
    const params: ListObjectsV2CommandInput = {
      Bucket: bucketName,
      Prefix: prefix,
      ContinuationToken: continuationToken,
    };

    const command = new ListObjectsV2Command(params);
    const response = await s3Client.send(command);

    // Collect all keys from the current batch
    if (response.Contents) {
      response.Contents.forEach((item) => {
        if (item.Key) {
          allKeys.push(item.Key);
        }
      });
    }

    // Update the continuation token
    continuationToken = response.NextContinuationToken;
  } while (continuationToken);

  return allKeys;
}

async function processBatch(
  batch: string[],
  bucket: string,
  prefix: string,
  outputDir: string,
  decompress: boolean = false
) {
  const downloadPromises = batch.map(async (key) => {
    const filename = key.replace(prefix, "");
    const localFilePath = path.join(outputDir, filename);
    const dir = path.dirname(localFilePath);
    await fsPromises.mkdir(dir, { recursive: true });
    return downloadFile(bucket, key, localFilePath, decompress);
  });

  const results = await Promise.allSettled(downloadPromises);
  results.forEach((result, index) => {
    if (result.status === "rejected") {
      log.error(`Download failed for ${batch[index]}: ${result.reason}`);
    }
  });
}

export async function downloadAllFilesFromPrefix(
  bucket: string,
  prefix: string,
  outputDir: string,
  batchSize: number = 10,
  decompress: boolean = false
): Promise<void> {
  try {
    log.info(
      `Downloading all files with prefix ${prefix} from storage bucket: ${bucket}`
    );
    const allKeys = await listAllS3Objects(bucket, prefix);
    log.info(`Found ${allKeys.length} files to download`);

    // Download files in batches
    for (let i = 0; i < allKeys.length; i += batchSize) {
      const batch = allKeys.slice(i, i + batchSize);
      await processBatch(batch, bucket, prefix, outputDir, decompress);
    }

    log.info(
      `All files from s3://${bucket}/${prefix} downloaded to ${outputDir} successfully`
    );
  } catch (err) {
    log.error("Error downloading files: ", err);
  }
}

export async function uploadDirectory(
  directory: string,
  bucket: string,
  prefix: string,
  batchSize: number = 10,
  compress: boolean = false
): Promise<void> {
  try {
    log.info(`Uploading directory ${directory} to storage bucket: ${bucket}`);
    const fileList = await getAllFilePaths(directory);
    log.info(`Found ${fileList.length} files to upload`);
    for (let i = 0; i < fileList.length; i += batchSize) {
      const batch = fileList.slice(i, i + batchSize);
      await Promise.all(
        batch.map(async (filePath) => {
          const localFilePath = path.join(directory, filePath);
          const key = prefix + filePath;
          return await uploadFile(localFilePath, bucket, key, compress);
        })
      );
    }
    log.info("Directory uploaded successfully");
  } catch (err) {
    log.error("Error uploading directory: ", err);
  }
}

async function getAllFilePaths(dir: string): Promise<string[]> {
  let fileList: string[] = [];

  async function recurse(currentPath: string) {
    const entries = await fsPromises.readdir(currentPath, {
      withFileTypes: true,
    });
    for (let entry of entries) {
      const fullPath = path.join(currentPath, entry.name);
      if (entry.isDirectory()) {
        await recurse(fullPath);
      } else {
        fileList.push(path.relative(dir, fullPath));
      }
    }
  }

  await recurse(dir);
  return fileList;
}

export async function deleteFile(bucket: string, key: string): Promise<void> {
  try {
    log.info(`Deleting file s3://${bucket}/${key}`);
    const params = {
      Bucket: bucket,
      Key: key,
    };
    await s3Client.send(new DeleteObjectCommand(params));
    log.info("File deleted successfully");
  } catch (err) {
    log.error("Error deleting file: ", err);
  }
}
