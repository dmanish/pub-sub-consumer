# Problem

Hello!

As you've heard by now, Censys scans the internet at an incredible scale. Processing the results necessitates scaling horizontally across thousands of machines. One key aspect of our architecture is the use of distributed queues to pass data between machines.

---

The `docker-compose.yml` file sets up a toy example of a scanner. It spins up a Google Pub/Sub emulator, creates a topic and subscription, and publishes scan results to the topic. It can be run via `docker compose up`.

Your job is to build the data processing side. It should:
1. Pull scan results from the subscription `scan-sub`.
2. Maintain an up-to-date record of each unique `(ip, port, service)`. This should contain when the service was last scanned and a string containing the service's response.

> **_NOTE_**
The scanner can publish data in two formats, shown below. In both of the following examples, the service response should be stored as: `"hello world"`.
> ```javascript
> {
>   // ...
>   "data_version": 1,
>   "data": {
>     "response_bytes_utf8": "aGVsbG8gd29ybGQ="
>   }
> }
>
> {
>   // ...
>   "data_version": 2,
>   "data": {
>     "response_str": "hello world"
>   }
> }
> ```

Your processing application should be able to be scaled horizontally, but this isn't something you need to actually do. The processing application should use `at-least-once` semantics where ever applicable.

You may write this in any languages you choose, but Go, Scala, or Rust would be preferred. You may use any data store of your choosing, with `sqlite` being one example.

--- 

Please upload the code to a publicly accessible GitHub, GitLab or other public code repository account.  This README file should be updated, briefly documenting your solution. Like our own code, we expect testing instructions: whether it’s an automated test framework, or simple manual steps.

To help set expectations, we believe you should aim to take no more than 4 hours on this task.

We understand that you have other responsibilities, so if you think you’ll need more than 5 business days, just let us know when you expect to send a reply.

Please don’t hesitate to ask any follow-up questions for clarification.

# How to run 
Censys Scan Data Processor (Go)
This Go application processes simulated scan results from a Google Pub/Sub emulator, maintaining an up-to-date record of unique (ip, port, service) combinations in an SQLite database.

How to Use in GitHub Codespaces
Follow these steps to run the data processor and verify its functionality within a GitHub Codespace.

1. Codespace Setup
Fork this repository (dmanish/mini-scan-base) on GitHub.

Create a Codespace from your forked repository.

Navigate to the processor/ directory within your Codespace. Your Go application code should already be present here.

Initialize Go module and download dependencies in the processor/ directory:

cd processor
go mod tidy

2. Start the Pub/Sub Emulator and Scanner
Open a new terminal in your Codespace (Terminal > New Terminal).

Navigate to the root of the mini-scan-base repository (e.g., /workspaces/mini-scan-base).

Start the Docker Compose services:

docker compose up

This will spin up the Pub/Sub emulator and the scanner, which will begin publishing messages. Keep this terminal running.

3. Run the Go Data Processor
Go back to your first terminal (or open another new one).

Navigate to your Go application directory:

cd processor

Set the Pub/Sub emulator host environment variable:

export PUBSUB_EMULATOR_HOST="localhost:8085"

(Codespaces automatically handles port forwarding for Docker services.)

Run your Go application:

go run main.go

#How to check DB on command line
Open a new terminal in your Codespace.

Navigate to your processor/ directory:

cd processor

Connect to the database:

sqlite3 scan_records.db

Query the data:

.headers on
.mode column
SELECT * FROM scan_records;

Exit SQLite:

.quit

# Clean Up
To stop the services and clean up:

Press Ctrl+C in the terminal running your Go application.

Press Ctrl+C in the terminal running docker compose up.

(Optional) To remove Docker containers and volumes:

cd /workspaces/mini-scan-base # Go to the root of the repo
docker compose down -v

(Optional) To remove the SQLite database file:

rm processor/scan_records.db


You should see logs indicating that the processor is starting, connecting to Pub/Sub, and processing messages. The retry mechanism will wait for the scan-sub subscription to become available.


