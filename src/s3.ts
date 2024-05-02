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

const { AWS_REGION, AWS_DEFAULT_REGION } = process.env;

const s3Client = new S3Client({ region: AWS_REGION || AWS_DEFAULT_REGION });

export async function uploadFile(
  localFilePath: string,
  bucketName: string,
  key: string
): Promise<void> {
  try {
    console.log(
      `Uploading file ${localFilePath} to storage bucket: ${bucketName}/${key}`
    );
    // Create a stream from the local file
    const fileStream = fs.createReadStream(localFilePath);

    // Set up the upload parameters
    const uploadParams = {
      Bucket: bucketName,
      Key: key,
      Body: fileStream,
    };

    // Perform the upload
    const parallelUploads3 = new Upload({
      client: s3Client,
      params: uploadParams,
    });

    // Track progress
    parallelUploads3.on("httpUploadProgress", (progress: Progress) => {
      console.log(`Uploaded ${progress.loaded} out of ${progress.total} bytes`);
    });

    // Wait for the upload to finish
    await parallelUploads3.done();
    console.log("Upload completed successfully");
  } catch (err) {
    console.error("Error uploading file: ", err);
  }
}

export async function downloadFile(
  bucketName: string,
  key: string,
  localFilePath: string
): Promise<void> {
  try {
    console.log(
      `Downloading file ${localFilePath} from storage bucket: ${bucketName}/${key}`
    );
    // Set up the download parameters
    const downloadParams = {
      Bucket: bucketName,
      Key: key,
    };

    // Perform the download
    const data = await s3Client.send(new GetObjectCommand(downloadParams));

    return new Promise((resolve, reject) => {
      if (data.Body instanceof Readable) {
        // Loop through body chunks and write to file
        const writeStream = fs.createWriteStream(localFilePath);
        data.Body.pipe(writeStream)
          .on("error", (err: any) => reject(err))
          .on("close", () => resolve());
      }
    });
  } catch (err: any) {
    console.error("Error downloading file: ", err);
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
  outputDir: string
) {
  const downloadPromises = batch.map(async (key) => {
    const filename = key.replace(prefix, "");
    const localFilePath = path.join(outputDir, filename);
    const dir = path.dirname(localFilePath);
    await fsPromises.mkdir(dir, { recursive: true });
    return downloadFile(bucket, key, localFilePath);
  });

  const results = await Promise.allSettled(downloadPromises);
  results.forEach((result, index) => {
    if (result.status === "rejected") {
      console.error(`Download failed for ${batch[index]}: ${result.reason}`);
    }
  });
  console.log(
    `Batch processed with ${
      results.filter((r) => r.status === "fulfilled").length
    } successes and ${
      results.filter((r) => r.status === "rejected").length
    } failures.`
  );
}

export async function downloadAllFilesFromPrefix(
  bucket: string,
  prefix: string,
  outputDir: string,
  batchSize: number = 10
): Promise<void> {
  try {
    console.log(
      `Downloading all files with prefix ${prefix} from storage bucket: ${bucket}`
    );
    const allKeys = await listAllS3Objects(bucket, prefix);
    console.log(`Found ${allKeys.length} files to download`);

    // Download files in batches
    for (let i = 0; i < allKeys.length; i += batchSize) {
      const batch = allKeys.slice(i, i + batchSize);
      await processBatch(batch, bucket, prefix, outputDir);
    }

    console.log(
      `All files from s3://${bucket}/${prefix} downloaded to ${outputDir} successfully`
    );
  } catch (err) {
    console.error("Error downloading files: ", err);
  }
}

export async function uploadDirectory(
  directory: string,
  bucket: string,
  prefix: string,
  batchSize: number = 10
): Promise<void> {
  try {
    console.log(
      `Uploading directory ${directory} to storage bucket: ${bucket}`
    );
    const fileList = await getAllFilePaths(directory);
    console.log(`Found ${fileList.length} files to upload`);
    for (let i = 0; i < fileList.length; i += batchSize) {
      const batch = fileList.slice(i, i + batchSize);
      await Promise.all(
        batch.map(async (filePath) => {
          const localFilePath = path.join(directory, filePath);
          const key = prefix + filePath;
          return await uploadFile(localFilePath, bucket, key);
        })
      );
    }
    console.log("Directory uploaded successfully");
  } catch (err) {
    console.error("Error uploading directory: ", err);
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
    console.log(`Deleting file s3://${bucket}/${key}`);
    const params = {
      Bucket: bucket,
      Key: key,
    };
    await s3Client.send(new DeleteObjectCommand(params));
    console.log("File deleted successfully");
  } catch (err) {
    console.error("Error deleting file: ", err);
  }
}
