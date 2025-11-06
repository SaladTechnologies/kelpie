import {
  S3Client,
  GetObjectCommand,
  ListObjectsV2Command,
  ListObjectsV2CommandInput,
  DeleteObjectCommand,
} from "@aws-sdk/client-s3";
import { Progress, Upload } from "@aws-sdk/lib-storage";
import { NodeHttpHandler } from "@smithy/node-http-handler";
import fs from "fs";
import { Readable } from "stream";
import path from "path";
import fsPromises from "fs/promises";
import { createGzip, createGunzip } from "zlib";
import { Logger } from "pino";
import { SyncConfig } from "./types";
import state from "./state";
import mime from "mime-types";

const { AWS_REGION, AWS_DEFAULT_REGION } = process.env;

const s3Client = new S3Client({
  region: AWS_REGION || AWS_DEFAULT_REGION,
  requestHandler: new NodeHttpHandler({
    requestTimeout: 0,
    connectionTimeout: 10000,
  }),
});

function guessMimeType(filePath: string): string {
  const mimeType =
    mime.lookup(path.extname(filePath)) || "application/octet-stream";
  return mimeType;
}

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

export async function uploadFile(
  jobId: string,
  localFilePath: string,
  bucketName: string,
  key: string,
  compress: boolean = false,
  log: Logger
): Promise<void> {
  try {
    if (state.hasUpload(jobId, localFilePath)) {
      log.info(`Upload of ${localFilePath} already in progress`);
      log.debug(state.getJSONState());
      return;
    }
    await state.startUpload(jobId, localFilePath, log);
    log.info(`Uploading ${localFilePath} to s3://${bucketName}/${key}`);
    // Create a stream from the local file
    const fileStream = fs.createReadStream(localFilePath);
    let stream;
    if (compress) {
      log.debug(`Compressing ${localFilePath} file with gzip`);
      const gzipStream = createGzip({ level: 9 });
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
      ContentType: guessMimeType(localFilePath),
      ContentEncoding: compress ? "gzip" : undefined,
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
  } catch (err: any) {
    log.error("Error uploading file: ", err);
  }
  await state.finishUpload(jobId, localFilePath, log);
  log.debug(state.getJSONState());
}

export async function downloadFile(
  jobId: string,
  bucketName: string,
  key: string,
  localFilePath: string,
  decompress: boolean = false,
  log: Logger
): Promise<void> {
  try {
    const start = Date.now();
    if (state.hasDownload(jobId, key)) {
      log.info(`Download of ${key} already in progress`);
      return;
    }
    state.startDownload(jobId, localFilePath, log);

    const isGzipped = decompress && key.endsWith(".gz");
    if (isGzipped) {
      localFilePath = localFilePath.replace(/\.gz$/, "");
    }
    // Set up the download parameters
    const downloadParams = {
      Bucket: bucketName,
      Key: key,
    };

    // Get the object from S3, which returns a stream
    const data = await s3Client.send(new GetObjectCommand(downloadParams));

    return new Promise((resolve, reject) => {
      if (data.Body instanceof Readable) {
        /**
         * S3 gives us a stream back from our request, which can pipe through
         * the various steps it needs to take to get to the final file.
         */
        let stream: Readable = data.Body;
        if (isGzipped) {
          log.debug(`Decompressing ${key} file with gunzip`);
          const unzipStream = createGunzip();
          unzipStream.on("error", async (err) => {
            log.error(`Error decompressing ${key} file: ${err}`);
            state.finishDownload(jobId, localFilePath, log);
            reject(err);
          });
          /**
           * If we've determined that the file is gzipped, we'll pipe the stream
           * directly through the gunzip stream to decompress it.
           */
          stream = stream.pipe(unzipStream);
        }

        /**
         * At this point we have a decompressed stream that we can pipe directly
         * to the file system to write the file.
         */
        const writeStream = fs.createWriteStream(localFilePath);
        stream
          .pipe(writeStream)
          .on("error", (err: any) => {
            log.error(`Error writing to ${localFilePath}: ${err}`);
            state.finishDownload(jobId, localFilePath, log);
            reject(err);
          })
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
            state.finishDownload(jobId, localFilePath, log);
            resolve();
          });
      }
    });
  } catch (err: any) {
    log.error("Error downloading file: ", err);
    await state.finishDownload(jobId, localFilePath, log);
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
  jobId: string,
  batch: string[],
  bucket: string,
  prefix: string,
  outputDir: string,
  decompress: boolean = false,
  log: Logger
) {
  const downloadPromises = batch.map(async (key) => {
    const filename = key.replace(prefix, "");
    const localFilePath = path.join(outputDir, filename);
    const dir = path.dirname(localFilePath);
    await fsPromises.mkdir(dir, { recursive: true });
    return downloadFile(jobId, bucket, key, localFilePath, decompress, log);
  });

  const results = await Promise.allSettled(downloadPromises);
  results.forEach((result, index) => {
    if (result.status === "rejected") {
      log.error(`Download failed for ${batch[index]}: ${result.reason}`);
    }
  });
}

/**
 * Downloads all files with a given prefix from an S3 bucket to a local directory,
 * and with an additional regular expression filter.
 *
 * This function downloads files in batches to improve performance. The number of files
 * downloaded in parallel can be controlled with the `batchSize` parameter.
 *
 * If the `decompress` parameter is set to true, the function will decompress the files
 * while downloading them.
 *
 * @param options.jobId The ID of the job
 * @param options.bucket The name of the S3 bucket to download from
 * @param options.prefix The prefix of the files to download
 * @param options.outputDir The local directory to download the files to
 * @param options.batchSize The number of files to download in parallel
 * @param options.decompress Whether to decompress the files after downloading
 * @param options.log The logger to use for output
 * @param options.pattern A regular expression to filter the files to download.
 */
export async function downloadAllFilesFromPrefix({
  jobId,
  bucket,
  prefix,
  outputDir,
  batchSize = 10,
  decompress = false,
  log,
  pattern,
}: {
  jobId: string;
  bucket: string;
  prefix: string;
  outputDir: string;
  batchSize: number;
  decompress: boolean;
  log: Logger;
  pattern?: RegExp;
}): Promise<void> {
  try {
    log.info(
      `Downloading all files with prefix ${prefix} from storage bucket: ${bucket}`
    );
    let allKeys = await listAllS3Objects(bucket, prefix);
    if (pattern) {
      log.debug(`Filtering files with pattern: ${pattern}`);
      allKeys = allKeys.filter((key) => pattern.test(key));
    }
    log.info(`Found ${allKeys.length} files to download`);

    // Download files in batches
    for (let i = 0; i < allKeys.length; i += batchSize) {
      const batch = allKeys.slice(i, i + batchSize);
      await processBatch(
        jobId,
        batch,
        bucket,
        prefix,
        outputDir,
        decompress,
        log
      );
    }

    log.info(
      `All files from s3://${bucket}/${prefix} downloaded to ${outputDir} successfully`
    );
  } catch (err: any) {
    log.error("Error downloading files: ", err);
  }
}

/**
 * Uploads all files from a local directory to an S3 bucket with a given prefix.
 * The function uploads files in batches to improve performance. The number of files
 * uploaded in parallel can be controlled with the `batchSize` parameter. Files are themselves
 * uploaded in parallel using the `Upload` class from the `@aws-sdk/lib-storage` package. The
 * intention is to fully utilize the available bandwidth and improve the overall upload speed.
 *
 * If the `compress` parameter is set to true, the function will compress the files
 * while uploading them.
 *
 * The `pattern` parameter can be used to filter the files to upload using a regular
 * expression.
 *
 *
 *
 * @param param0
 * @returns
 */
export async function uploadDirectory({
  jobId,
  directory,
  bucket,
  prefix,
  batchSize = 10,
  compress = false,
  log,
  pattern,
}: {
  jobId: string;
  directory: string;
  bucket: string;
  prefix: string;
  batchSize: number;
  compress: boolean;
  log: Logger;
  pattern?: RegExp;
}): Promise<void> {
  try {
    log.info(`Uploading directory ${directory} to storage bucket: ${bucket}`);
    let fileList = await getAllFilePaths(directory);
    if (pattern) {
      log.info(`Filtering files with pattern: ${pattern}`);
      fileList = fileList.filter((key) => pattern.test(key));
    }
    if (fileList.length === 0) {
      log.info("No files found to upload");
      return;
    }
    log.info(`Found ${fileList.length} files to upload`);
    for (let i = 0; i < fileList.length; i += batchSize) {
      const batch = fileList.slice(i, i + batchSize);
      await Promise.all(
        batch.map(async (filePath) => {
          const localFilePath = path.join(directory, filePath);
          const key = prefix + filePath;
          return await uploadFile(
            jobId,
            localFilePath,
            bucket,
            key,
            compress,
            log
          );
        })
      );
    }
    log.info("Directory uploaded successfully");
  } catch (err: any) {
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

export async function deleteFile(
  bucket: string,
  key: string,
  log: Logger
): Promise<void> {
  try {
    log.info(`Deleting file s3://${bucket}/${key}`);
    const params = {
      Bucket: bucket,
      Key: key,
    };
    await s3Client.send(new DeleteObjectCommand(params));
    log.info("File deleted successfully");
  } catch (err: any) {
    log.error("Error deleting file: ", err);
  }
}

export async function downloadSyncConfig(
  jobId: string,
  config: SyncConfig,
  compression: boolean,
  log: Logger
) {
  const { bucket, prefix, local_path, direction, pattern } = config;
  if (direction === "download") {
    await downloadAllFilesFromPrefix({
      jobId,
      bucket,
      prefix,
      outputDir: local_path,
      batchSize: 20,
      decompress: compression,
      log,
      pattern: pattern ? new RegExp(pattern) : undefined,
    });
  }
}

export async function uploadSyncConfig(
  jobId: string,
  config: SyncConfig,
  compression: boolean,
  log: Logger
) {
  const { local_path, bucket, prefix, direction, pattern } = config;
  if (direction === "upload") {
    await uploadDirectory({
      jobId,
      directory: local_path,
      bucket,
      prefix,
      batchSize: 20,
      compress: compression,
      log,
      pattern: pattern ? new RegExp(pattern) : undefined,
    });
  }
}
