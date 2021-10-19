# How to run on GCP
1. Pull all 3 images into GCP
2. Tag them with your gcp registry and project name
3. push these images to the gcp docker image registry
4. Navigate to the clusters tab and ssh into the cluster
    1. or create a cluster and then ssh into it
5. From this project directory upload these files from the 'resource-manifests' directory to the gcp cloud shell
    1. sa-frontend-deployment.yaml
    2. sa-logic-deployment.yaml
    3. sa-web-app-deployment.yaml
    4. service-sa-frontend-lb.yaml
    5. service-sa-logic.yaml
    6. service-sa-web-app.yaml
6. run 'minikube start'
7. run kubectl create -f 'filename' and replace filename with the 6 files above
8. Restart cloud shell. (This step may be optional as I'm not sure if it wasn't just my client where sa-frontend-lb wouldn't expose an external ip without a restart)
9. Run 'minikube service sa-frontend-lb' and that should open the app.