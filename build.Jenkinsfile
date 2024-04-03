@Library('sailpoint/jenkins-release-utils')_
pipeline {
    
    parameters {
        string(name: 'BRANCH', defaultValue: 'master')
    }
    
    agent {
        kubernetes {
            yaml "${libraryResource 'pods/build-container.yaml'}"
        }
    }
    
    environment {
        
        SERVICE_NAME = "feast-bytewax"
        GITHUB_REPO = "git@github.com:sailpoint/feast.git"
        ECR_REPOSITORY = "${env.AWS_ACCOUNT_ID}.dkr.ecr.us-east-1.amazonaws.com"
        REPOSITORY_NAME = "sailpoint/${SERVICE_NAME}"
    }
    
    stages {
        stage('Checkout SCM') {
            steps {
                checkout(
                [$class: 'GitSCM',
                branches: [[name: "origin/${BRANCH}"], [name: "*/master"]],
                doGenerateSubmoduleConfigurations: false,
                extensions: [], submoduleCfg: [],
                userRemoteConfigs: [[credentialsId: 'git-automation-ssh', url: "$GITHUB_REPO"]]])
            }
        }
        stage('Build Docker') {
            steps {
                container('kaniko') {
                    script {
                        echo "publishing ${SERVICE_NAME}:${BUILD_NUMBER}"

                        sh """
                            /kaniko/executor \
                                --context ./ \
                                --dockerfile ./sailpoint-bytewax.dockerfile \
                                --build-arg  DOCKER_BUILDKIT=1 \
                                --destination=${ECR_REPOSITORY}/${REPOSITORY_NAME}:${BUILD_NUMBER}
                        """
                    }
                }
            }
        }
    }
    post {
        always {
            junit allowEmptyResults: true, testResults: '**/cov/*.xml'
        }
        success {
            container('aws-cli') {
                echo "Rebasing tag 'latest' to be '${BUILD_NUMBER}'"
                sh '''
                    MANIFEST=`aws ecr batch-get-image --repository-name ${REPOSITORY_NAME} --image-ids imageTag=${BUILD_NUMBER}  --query images[].imageManifest --output text`
                    aws ecr put-image --repository-name ${REPOSITORY_NAME} --image-tag latest --image-manifest "\$MANIFEST"
                '''
            }
            slackSend(
                channel       : "proj-eng-iai-cicd",
                message       : "${SERVICE_NAME}:${BUILD_NUMBER} Build \n ${JOB_URL} ",
                replyBroadcast: true,
                color         : "#33ff33",
                failOnError   : false
            )
        }
        failure {
            container('aws-cli') {
                assumePodRole {
                    script {
                       sh '''#!/bin/bash
                            aws ecr batch-delete-image \
                                --repository-name $REPOSITORY_NAME \
                                --image-ids imageTag=$BUILD_NUMBER
                          '''
                    }
                }
            }

            slackSend(
                channel       : "proj-eng-iai-cicd",
                message       : "@iai-tarragon ${SERVICE_NAME}:${BUILD_NUMBER} Build broken \n ${JOB_URL} ",
                replyBroadcast: true,
                color         : "#ff3333",
                failOnError   : false
            )
        }
    }
}
