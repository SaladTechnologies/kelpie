![](./kelpie.png)
# 🐕 Kelpie (beta)

Kelpie shepherds long-running jobs through to completion on interruptible hardware, coordinating with the [Kelpie API](https://github.com/SaladTechnologies/kelpie-api)

- [🐕 Kelpie (beta)](#-kelpie-beta)
  - [Who is it for?](#who-is-it-for)
  - [How it Works](#how-it-works)
  - [Adding the kelpie Worker To Your Container Image](#adding-the-kelpie-worker-to-your-container-image)
  - [Deploying Your Container Group](#deploying-your-container-group)
  - [What it DOES NOT do](#what-it-does-not-do)
  - [API Authorization](#api-authorization)
    - [Base URL](#base-url)
  - [Queueing a job](#queueing-a-job)
    - [`POST /jobs`](#post-jobs)
  - [Canceling a job](#canceling-a-job)
    - [`DELETE /jobs/:id`](#delete-jobsid)
  - [Checking on a job](#checking-on-a-job)
    - [`GET /jobs/:id`](#get-jobsid)
  - [Listing Your Jobs](#listing-your-jobs)
    - [`GET /jobs`](#get-jobs)
  - [Job Lifecycle](#job-lifecycle)
  - [Status Webhooks](#status-webhooks)
    - [Webhook Authorization](#webhook-authorization)

## Who is it for?

Kelpie is for anyone who wants to run long running compute-intensive jobs on [Salad](https://salad.com/), the world's largest distributed GPU cloud. Whether that's [LoRA training](https://blog.salad.com/cost-effective-stable-diffusion-fine-tuning-on-salad/), Monte Carlo simulations, Molecular Dynamics simulations, or anything else, Kelpie can help you run your jobs to completion, even if they take days or weeks.
You bring your own docker container that contains your script and dependencies, add the Kelpie binary to it, and deploy.

If you'd like to join the Kelpie beta, and are an existing Salad customer, just reach out to your point of contact via email, discord, or slack. If you're interested in Kelpie and are new to Salad, [sign up for a demo](https://salad.com/get-a-demo), and mention you're interested in using Kelpie.

## How it Works

![Kelpie Diagram](./kelpie-architecture.png)``s

Kelpie is a standalone binary that runs in your container image. It coordinates with the Kelpie API to download your input data, upload your output data, and sync progress checkpoints to your s3-compatible storage. You submit jobs to the [Kelpie API](https://kelpie.saladexamples.com/docs), and those jobs get assigned to salad worker nodes that have the Kelpie binary installed. 

If you define [scaling rules](https://kelpie.saladexamples.com/docs#/default/post_CreateScalingRule), the Kelpie API will handle starting and stopping your container group, and scaling it up and down in response to job volume.

When a job is assigned to a worker, the worker downloads your input data, and your checkpoint, and runs your command with the provided arguments and environment variables. When files are added to a directory defined in your job definition, Kelpie uploads that file to the bucket and prefix you've defined. When your command exits successfully, the output directory you defined is uploaded to your storage, the job is marked as complete, and a webhook is sent to the url you've provided, if any.

## Adding the kelpie Worker To Your Container Image

You can find a working example [here](https://github.com/SaladTechnologies/kelpie-demo)

```dockerfile
# Start with a base image that has the dependencies you need,
# and can successfully run your script.
FROM yourimage:yourtag

# Add the kelpie binary to your container image
ADD https://github.com/SaladTechnologies/kelpie/releases/download/0.4.2/kelpie /kelpie
RUN chmod +x /kelpie

# Use kelpie as the "main" command. Kelpie will then execute your
# command with the provided arguments and environment variables
# from the job definition.
CMD ["/kelpie"]
```

When running the image, you will need additional configuration in the environment:

- AWS/Cloudflare Credentials: Provide `AWS_ACCESS_KEY_ID`, etc to enable the kelpie worker to upload and download from your bucket storage. We use the s3 compatability api, so any s3-compatible storage should work.
- `KELPIE_API_URL`: the root URL for the coordination API, e.g. kelpie.saladexamples.com
- `KELPIE_API_KEY`: Your api key for the coordination API, issued by Salad for use with kelpie. NOT your Salad API Key.

Additionally, your script must support the following things:

- Saving and Resuming From Checkpoints: Your script should periodically output progress checkpoints to `CHECKPOINT_DIR`, so that the job can be resumed if it gets interrupted. Similarly, when your script starts, it should check `CHECKPOINT_DIR` to see if there is anything to resume, and only start from the beginning if no checkpoint is present.
- It must exit "successfully" with an exit code of 0 upon completion.
- (Deprecated) Environment variables - If these are set by you in your container group configuration, they will be respected, otherwise they will be set by kelpie. This functionality has been replaced by the `sync` block in the job definition.
  - `INPUT_DIR`: Where to look for whatever data is needed as input. This will be downloaded from your bucket storage by kelpie prior to running the script.
  - `CHECKPOINT_DIR`: This is where to save progress checkpoints locally. kelpie will handle syncing the contents to your bucket storage, and will make sure any existing checkpoint is downloaded prior to running the script.
  - `OUTPUT_DIR`: This is where to save any output artifacts. kelpie will upload your artifacts to your bucket storage.

Upload your docker image to the container registry of your choice. Salad supports public and private registries, including Docker Hub, AWS ECR, and GitHub Container Registry, among others.

## Deploying Your Container Group

You can deploy your container group using the [Salad API](https://docs.salad.com/api-reference/container_groups/create-a-container-group), or via the [Salad Portal](https://portal.salad.com/). You will need to add the kelpie salad user (currently shawn.rushefsky@salad.com) to your organization to enable the scaling features of kelpie. Kelpie uses the Salad API to start, stop, and scale your container group in response to job volume.

In your container group configuration, you will provide the docker image url, the hardware configuration needed by your job, and the environment variables detailed above. You do not need to enable Container Gateway, or Job Queues, and you do not need to configure probes. While Salad does offer built-in logging, it is still recommended to connect an external logging service for more advanced features.

Once your container group is deployed, and you've verified that the node starts and runs successfully, you'll want to retrieve the container group ID from the [Salad API](https://docs.salad.com/api-reference/container_groups/get-a-container-group). You will use this ID when submitting jobs to the Kelpie API.

## What it DOES NOT do

1. kelpie does not store your data on our servers or in our storage buckets, beyond the job definition you submit. It merely facilitates syncing your data from local node storage to your preferred s3-compatible storage.
2. kelpie does not monitor the ongoing progress of your job, beyond ensuring it eventually exits successfully. You should integrate your own monitoring solution, e.g. [Weights and Balances](https://wandb.ai/)
3. kelpie does not containerize your job for you. It provides a binary that can be added to existing containerized jobs.
4. kelpie does not create or delete your container groups. If configured with scaling rules, kelpie can start, stop, and scale your container group in response to job volume.

## API Authorization
There are live swagger docs that should be considered more accurate and up to date than this readme: https://kelpie.saladexamples.com/docs

Your kelpie api key is used by you to submit work, and also by kelpie workers to pull and process work.

All requests to the Kelpie API must include the header:

`X-Kelpie-Key: myapikey`

### Base URL

All API requests should use a base url of `https://kelpie.saladexamples.com`.

## Queueing a job

Queueing a job for processing is a post request to the Kelpie API. You must provide a command to run, and optionally arguments, environment variables, and sync instructions. A job must also be assigned to a specific container group, using the container group id. You can get your container group id from the [Salad API.](https://docs.salad.com/api-reference/container_groups/get-a-container-group). You can optionally provide a webhook url to receive status updates about your job.

### `POST /jobs`

**Request Body**

```json
{
  "command": "python",
  "arguments": [
    "/path/to/main.py",
    "--arg",
    "value"
  ],
  "environment": { "SOME_VAR": "string"},
  "webhook": "https://myapi.com/kelpie-webhooks",
  "container_group_id": "97f504e8-6de6-4322-b5d5-1777a59a7ad3",
  "sync": {
    "before": [
      {
        "bucket": "string",
        "prefix": "string",
        "local_path": "string",
        "direction": "download",
        "pattern": "checkpoint-*"
      }
    ],
    "during": [
      {
        "bucket": "string",
        "prefix": "string",
        "local_path": "string",
        "direction": "upload",
        "pattern": "*.bin"
      }
    ],
    "after": [
      {
        "bucket": "string",
        "prefix": "string",
        "local_path": "string",
        "direction": "upload",
        "pattern": "*.bin"
      }
    ]
  }
}
```

**Response Body**

```json
{
  "id": "8b9c902c-7da6-4af3-be0b-59cd4487895a",
  "user_id": "your-user-id",
  "status": "pending",
  "created": "2024-04-19T18:53:31.000Z",
  "started": null,
  "completed": null,
  "canceled": null,
  "failed": null,
  "command": "python",
  "arguments": [
    "/path/to/main.py",
    "--arg",
    "value"
  ],
  "environment": { "SOME_VAR": "string"},
  "webhook": "https://myapi.com/kelpie-webhooks",
  "heartbeat": null,
  "num_failures": 0,
  "container_group_id": "97f504e8-6de6-4322-b5d5-1777a59a7ad3",
  "machine_id": null,
  "sync": {
    "before": [
      {
        "bucket": "string",
        "prefix": "string",
        "local_path": "string",
        "direction": "download",
        "pattern": "checkpoint-*"
      }
    ],
    "during": [
      {
        "bucket": "string",
        "prefix": "string",
        "local_path": "string",
        "direction": "upload",
        "pattern": "*.bin"
      }
    ],
    "after": [
      {
        "bucket": "string",
        "prefix": "string",
        "local_path": "string",
        "direction": "upload",
        "pattern": "*.bin"
      }
    ]
  }
}
```

## Canceling a job

You can cancel a job using the job id

### `DELETE /jobs/:id`

**Response Body**

```json
{
  "message": "Job canceled"
}
```

## Checking on a job

As mentioned above, Kelpie does not monitor the progress of your job, but it does track the status (pending, running, canceled, completed, failed). You can get a job using the job id:

### `GET /jobs/:id`

**Response Body**

```json
{
  "id": "8b9c902c-7da6-4af3-be0b-59cd4487895a",
  "user_id": "your-user-id",
  "status": "pending",
  "created": "2024-04-19T18:53:31.000Z",
  "started": null,
  "completed": null,
  "canceled": null,
  "failed": null,
  "command": "python",
  "arguments": [
    "/path/to/main.py",
    "--arg",
    "value"
  ],
  "webhook": "https://myapi.com/kelpie-webhooks",
  "heartbeat": null,
  "num_failures": 0,
  "container_group_id": "97f504e8-6de6-4322-b5d5-1777a59a7ad3",
  "machine_id": null,
  "sync": {
    "before": [
      {
        "bucket": "string",
        "prefix": "string",
        "local_path": "string",
        "direction": "download",
        "pattern": "checkpoint-*"
      }
    ],
    "during": [
      {
        "bucket": "string",
        "prefix": "string",
        "local_path": "string",
        "direction": "upload",
        "pattern": "*.bin"
      }
    ],
    "after": [
      {
        "bucket": "string",
        "prefix": "string",
        "local_path": "string",
        "direction": "upload",
        "pattern": "*.bin"
      }
    ]
  }
}
```

## Listing Your Jobs

Get your jobs in bulk.

### `GET /jobs`

**Query Parameters**

All query parameters for this endpoint are optional.

| name               | description                                            | default |
| ------------------ | ------------------------------------------------------ | ------- |
| status             | pending, running, completed, canceled, failed          | *none*  |
| container_group_id | query only jobs assigned to a specific container group | *none*  |
| page_size          | How many jobs to return per page                       | 100     |
| page               | Which page of jobs to query                            | 1       |
| asc                | Boolean. Sort by `created`, ascending                  | false   |

**Response Body**

```json
{
  "_count": 1,
  "jobs": [
    {
      "id": "8b9c902c-7da6-4af3-be0b-59cd4487895a",
      "user_id": "your-user-id",
      "status": "pending",
      "created": "2024-04-19T18:53:31.000Z",
      "started": null,
      "completed": null,
      "canceled": null,
      "failed": null,
      "command": "python",
      "arguments": [
        "/path/to/main.py",
        "--arg",
        "value"
      ],
      "webhook": "https://myapi.com/kelpie-webhooks",
      "heartbeat": null,
      "num_failures": 0,
      "container_group_id": "97f504e8-6de6-4322-b5d5-1777a59a7ad3",
      "machine_id": null,
      "sync": {
        "before": [
          {
            "bucket": "string",
            "prefix": "string",
            "local_path": "string",
            "direction": "download",
            "pattern": "checkpoint-*"
          }
        ],
        "during": [
          {
            "bucket": "string",
            "prefix": "string",
            "local_path": "string",
            "direction": "upload",
            "pattern": "*.bin"
          }
        ],
        "after": [
          {
            "bucket": "string",
            "prefix": "string",
            "local_path": "string",
            "direction": "upload",
            "pattern": "*.bin"
          }
        ]
      }
    }
  ]
}
```


## Job Lifecycle

1. When kelpie starts on a new node, it starts polling for available work from `/work`. In these requests, it includes some information about what salad node you're on, including the machine id and container group id. This ensures we only hand out work to the correct container group, and that we do not hand out to a machine where that job has previously failed.
2. When a job is started, a webhook is sent, if configured.
3. Once it receives a job, kelpie downloads your inputs, and your checkpoint
4. Once required files are downloaded, kelpie executes your command with the provided arguments, adding environment variables as documented above.
5. Whenever files are added to the checkpoint directory, kelpie syncs the directory to the checkpoint bucket and prefix.
6. Whenever files are added to the output directory, kelpie syncs the directory to the output bucket and prefix.
7. When your command exits 0, the job is marked as complete, and a webhook is sent (if configured) to notify you about the job's completion.
   1. If your job fails, meaning exits non-0, it will be reported as a failure to the api. When this occurs, the number of failures for the job is incremented, up to 3. The machine id reporting the failure will be blocked from receiving that job again. If the job fails 3 times, it is marked failed, and a webhook is sent, if configured. If a machine id is blocked from 5 jobs, the container will be reallocated to a different machine, provided you have added the kelpie user to your salad org.
8. input, checkpoint, and output directories are purged, and the cycle begins again

## Status Webhooks

If you provide a url in the webhook field, the Kelpie API will send status webhooks. It makes a `POST` request to the url provided, with a JSON request body:

```json
{
  "status": "running",
  "job_id": "some-job-id",
  "machine_id": "some-machine-id",
  "container_group_id": "some-container-group-id"
}
```

Webhook status may be `running`, `failed`, or `completed`

### Webhook Authorization

Webhooks sent by the Kelpie API will be secured with your API token in the `X-Kelpie-Key` header.
