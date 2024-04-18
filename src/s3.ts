import {
  S3Client,
  GetObjectCommand,
  ListObjectsCommand,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import { Progress, Upload } from "@aws-sdk/lib-storage";
import fs from "fs";
import { pipeline, Readable } from "stream";
import { promisify } from "util";
import path from "path";
import fsPromises from "fs/promises";

const { AWS_REGION, AWS_DEFAULT_REGION } = process.env;

const s3Client = new S3Client({ region: AWS_REGION || AWS_DEFAULT_REGION });

const pipelineAsync = promisify(pipeline);

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

    if (data.Body) {
      const readableStream = new Readable().wrap(
        data.Body as NodeJS.ReadableStream
      );
      const fileStream = fs.createWriteStream(localFilePath);
      await pipelineAsync(readableStream, fileStream);
    }

    console.log("Download completed successfully");
  } catch (err) {
    console.error("Error downloading file: ", err);
  }
}

export async function downloadAllFilesFromPrefix(
  bucket: string,
  prefix: string,
  outputDir: string
): Promise<void> {
  try {
    console.log(
      `Downloading all files with prefix ${prefix} from storage bucket: ${bucket}`
    );
    const listObjectsParams = {
      Bucket: bucket,
      Prefix: prefix,
    };

    const data = await s3Client.send(new ListObjectsCommand(listObjectsParams));

    if (data.Contents) {
      await Promise.all(
        data.Contents.map(async (object) => {
          const key = object.Key!;
          // The filename should remove the prefix from the key
          const filename = key.replace(prefix, "");
          if (!filename) {
            return;
          }
          const localFilePath = path.join(outputDir, filename);
          await downloadFile(bucket, key, localFilePath);
        })
      );
    }

    console.log("Download completed successfully");
  } catch (err) {
    console.error("Error downloading files: ", err);
  }
}

export async function uploadDirectory(
  directory: string,
  bucket: string,
  prefix: string
): Promise<void> {
  try {
    console.log(
      `Uploading directory ${directory} to storage bucket: ${bucket}`
    );
    const fileList = await getAllFilePaths(directory);
    await Promise.all(
      fileList.map(async (filePath) => {
        const localFilePath = path.join(directory, filePath);
        const key = prefix + filePath;
        await uploadFile(localFilePath, bucket, key);
      })
    );
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
