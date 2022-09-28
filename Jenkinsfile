pipeline {
  agent any

  environment {
    
    //WORKSPACE           = '.'
    //DBRKS_BEARER_TOKEN  = "xyz"
    GITHUBCREDID        = "2f32128c-e650-4942-8e7f-94bf18a82a95"
    GITREPOREMOTE       = "https://github.com/majumdarsubho/mdlp"
    CURRENTRELEASE      = "main"
    GITREPO             = "/var/lib/jenkins/workspace/${env.JOB_NAME}"
    DBTOKEN             = "databricks-token"
    CLUSTERID           = "0819-214501-chjkd9g9"
    DBURL               = "https://dbc-db420c65-4456.cloud.databricks.com"

    TESTRESULTPATH  ="${BUILDPATH}/Validation/reports/junit"
    LIBRARYPATH     = "${GITREPO}/Libraries"
    OUTFILEPATH     = "${BUILDPATH}/Validation/Output"
    NOTEBOOKPATH    = "${GITREPO}/Notebooks"
    WORKSPACEPATH   = "/Demo-notebooks"               //"/Shared"
    DBFSPATH        = "dbfs:/FileStore/"
    BUILDPATH       = "${WORKSPACE}/Builds/${env.JOB_NAME}-${env.BUILD_NUMBER}"
    SCRIPTPATH      = "${GITREPO}/Scripts"
    projectName = "${WORKSPACE}"  //var/lib/jenkins/workspace/Demopipeline/
    projectKey = "key"
 }

  stages {
    stage('Checkout') { // for display purposes
	    steps {
		  echo "Pulling ${CURRENTRELEASE} Branch from Github"
		  git branch: CURRENTRELEASE, credentialsId: GITHUBCREDID, url: GITREPOREMOTE
	    }
    }
	  
  }
	
  post {
		success {
		  withAWS(credentials:'AWSCredentialsForSnsPublish') {
				snsPublish(
					topicArn:'arn:aws:sns:us-east-1:872161624847:mdlp-build-status-topic', 
					subject:"Job:${env.JOB_NAME}-Build Number:${env.BUILD_NUMBER} is a ${currentBuild.currentResult}", 
					message: "Please note that for Jenkins job:${env.JOB_NAME} of build number:${currentBuild.number} - ${currentBuild.currentResult} happened!"
				)
			}
		}
		failure {
		  withAWS(credentials:'AWSCredentialsForSnsPublish') {
				snsPublish(
					topicArn:'arn:aws:sns:us-east-1:872161624847:mdlp-build-status-topic', 
					subject:"Job:${env.JOB_NAME}-Build Number:${env.BUILD_NUMBER} is a ${currentBuild.currentResult}", 
					message: "Please note that for Jenkins job:${env.JOB_NAME} of build number:${currentBuild.number} - ${currentBuild.currentResult} happened! Details here: ${BUILD_URL}."
				)
			}
		}
  }
}
