buildMvn {
  publishModDescriptor = 'yes'
  mvnDeploy = 'yes'
  publishAPI = 'yes'
  doKubeDeploy = true
  buildNode = 'jenkins-agent-java11'

  doApiLint = true
  apiTypes = 'RAML'
  apiDirectories = 'ramls'

  doDocker = {
    buildJavaDocker {
      publishMaster = 'yes'
      healthChk = 'no'
    }
  }
}

