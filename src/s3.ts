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
