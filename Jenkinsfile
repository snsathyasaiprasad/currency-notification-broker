pipeline {
    agent any

    environment {
        DOCKERHUB_USER = "snsathyasaiprasad"
        IMAGE_TAG = "${env.BUILD_NUMBER}"
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Build & Test - currency-service') {
            steps {
                dir('currency-service') {
                    sh 'mvn clean package -DskipTests'
                }
            }
        }

        stage('Build & Test - notification-service') {
            steps {
                dir('notification-service') {
                    sh 'mvn clean package -DskipTests'
                }
            }
        }

        stage('Docker Build & Push') {
            steps {
                withCredentials([usernamePassword(credentialsId: 'dockerhub-creds', usernameVariable: 'DUSER', passwordVariable: 'DPASS')]) {
                    sh '''
                        echo "$DPASS" | docker login -u "$DUSER" --password-stdin
                        docker build -t $DOCKERHUB_USER/currency-service:$IMAGE_TAG ./currency-service
                        docker build -t $DOCKERHUB_USER/notification-service:$IMAGE_TAG ./notification-service
                        docker push $DOCKERHUB_USER/currency-service:$IMAGE_TAG
                        docker push $DOCKERHUB_USER/notification-service:$IMAGE_TAG
                    '''
                }
            }
        }

        stage('Deploy to Kubernetes') {
            steps {
                withCredentials([file(credentialsId: 'kubeconfig', variable: 'KUBECONFIG')]) {
                    sh '''
                        sed -i "s|currency-service:.*|currency-service:$IMAGE_TAG|" k8s/03-currency-service.yaml
                        sed -i "s|notification-service:.*|notification-service:$IMAGE_TAG|" k8s/04-notification-service.yaml
                        kubectl apply -f k8s/00-namespace.yaml
                        kubectl apply -f k8s/01-postgres.yaml
                        kubectl apply -f k8s/02-kafka.yaml
                        kubectl apply -f k8s/03-currency-service.yaml
                        kubectl apply -f k8s/04-notification-service.yaml
                    '''
                }
            }
        }
    }

    post {
        success { echo 'Pipeline completed successfully.' }
        failure { echo 'Pipeline failed — check stage logs above.' }
    }
}
