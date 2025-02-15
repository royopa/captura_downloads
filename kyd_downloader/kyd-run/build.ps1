
gcloud builds submit --tag gcr.io/kyd-storage-001/kyd-process-file-pubsub
gcloud run deploy kyd-process-file --image gcr.io/kyd-storage-001/kyd-process-file-pubsub --memory 8G --cpu 2
