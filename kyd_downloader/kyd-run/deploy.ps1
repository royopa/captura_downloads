. ..\..\..\google-cloud\Setenv.ps1

gcloud config set project kyd-storage-001

gcloud config set run/region us-central1

gcloud pubsub topics create kyd-test

# build
gcloud builds submit --tag gcr.io/kyd-storage-001/kyd-process-file-pubsub

gcloud run deploy kyd-process-file --image gcr.io/kyd-storage-001/kyd-process-file-pubsub --memory 8G --cpu 2

gcloud auth print-identity-token | clip

gcloud iam service-accounts create cloud-run-pubsub-invoker --display-name "Cloud Run Pub/Sub Invoker"

gcloud run services add-iam-policy-binding kyd-process-file `
   --member=serviceAccount:cloud-run-pubsub-invoker@kyd-storage-001.iam.gserviceaccount.com `
   --role=roles/run.invoker

# Cloud Pub/Sub needs the role roles/iam.serviceAccountTokenCreator granted to service
# account service-420935162637@gcp-sa-pubsub.iam.gserviceaccount.com on this project to 
# create identity tokens. You can change this later.

gcloud pubsub subscriptions create run-kyd-process-file --topic kyd-storage-download-log `
   --push-endpoint=https://kyd-process-file-kb2piwuv2q-uc.a.run.app/ `
   --push-auth-service-account=cloud-run-pubsub-invoker@kyd-storage-001.iam.gserviceaccount.com

gcloud pubsub topics publish kyd-storage-download-log --message "[1]"
gcloud pubsub topics publish kyd-storage-download-log --message '{"message": "File saved", "download_status": 200, "status": 0, "refdate": "2022-01-20T00:00:00.000001-0300", "filename": "2022-01-20.txt", "bucket": "ks-rawdata-anbima-titpub", "name": "titpub_anbima", "time": "2022-01-21T21:07:47.156344+0000"}'

gcloud run services delete kyd-process-file

gcloud pubsub topics delete kyd-test

gcloud pubsub subscriptions delete run-kyd-process-file

gcloud container images delete gcr.io/kyd-storage-001/kyd-process-file-pubsub:latest --force-delete-tags

gcloud iam service-accounts delete cloud-run-pubsub-invoker@kyd-storage-001.iam.gserviceaccount.com

gcloud config unset run/region

gcloud config unset project
