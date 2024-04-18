# Kelpie (beta)

Kelpie shepherds long-running jobs through to completion on interruptible hardware, coordinating with the Kelpie API

## Adding the kelpie Worker To Your Container Image

```dockerfile
FROM yourimage:yourtag

# Install wget if it is not already present in your image
RUN apt-get install -y wget

# kelpie is a standalone x86-64 linux binary
RUN wget https://github.com/SaladTechnologies/kelpie/releases/download/0.0.1/kelpie -P / && chmod +x /kelpie

CMD ["/kelpie"]
```

When running the image, you will need additional configuration in the environment:

- AWS/Cloudflare Credentials: Provide `AWS_ACCESS_KEY_ID`, etc to enable the kelpie worker to upload and download from your bucket storage. We use the s3 compatability api, so any s3-compatible storage should work.
- `KELPIE_API_URL`: the root URL for the coordination API, e.g. kelpie.saladexamples.com
- `KELPIE_API_KEY`: Your api key for the coordination API, issued by Salad for use with kelpie. NOT your Salad API Key.


Additionally, your script must support the following things:

- Environment variables - If these are set by you in your container group configuration, they will be respected, otherwise they will be set by kelpie.
  - `INPUT_DIR`: Where to look for whatever data is needed as input. This will be downloaded from your bucket storage by kelpie prior to running the script.
  - `CHECKPOINT_DIR`: This is where to save progress checkpoints locally. kelpie will handle syncing the contents to your bucket storage, and will make sure any existing checkpoint is downloaded prior to running the script.
  - `OUTPUT_DIR`: This is where to save any output artifacts. kelpie will upload your artifacts to your bucket storage.
- Saving and Resuming From Checkpoints: Your script should periodically output progress checkpoints to `CHECKPOINT_DIR`, so that the job can be resumed if it gets interrupted. Similarly, when your script starts, it should check `CHECKPOINT_DIR` to see if there is anything to resume, and only start from the beginning if no checkpoint is present.
- It must exit "successfully" with an exit code of 0 upon completion.

## Queueing a job

Queueing a job for processing is a simple post request to the Kelpie API

`POST /job`

with the header:

`X-Kelpie-Key: myapikey`

with a JSON request body:

```json
{
  "command": "python",
  "arguments": [
    "/path/to/main.py"
    "--arg",
    "value"
  ],
  "input_bucket": "my-bucket",
  "input_prefix": "inputs/job1/",
  "checkpoint_bucket": "my-bucket",
  "checkpoint_prefix": "checkpoints/job1/",
  "output_bucket": "my-bucket",
  "output_prefix": "outputs/job1/",
  "webhook": "https://myapi.com/kelpie-webhooks",
  "container_group_id": "97f504e8-6de6-4322-b5d5-1777a59a7ad3"
}
```

## Job Lifecycle

1. When kelpie starts on a new node, it starts polling for available work from `/work`. In these requests, it includes some information about what salad node you're on, including the machine id and container group id. This ensures we only hand out work to the correct container group, and that we do not hand out to a machine where that job has previously failed.
2. Once it receives a job, kelpie downloads your inputs, and your checkpoint
3. Once required files are downloaded, kelpie executes your command with the provided arguments, adding environment variables as documented above.
4. Whenever files are added to the checkpoint directory, kelpie syncs the directory to the checkpoint bucket and prefix.
5. Whenever files are added to the output directory, kelpie syncs the directory to the output bucket and prefix.
6. When your command exits, the job is marked as complete, and a webhook is sent (if configured) to notify you about the job's completion.
7. input, checkpoint, and output directories are purged, and the cycle begins again