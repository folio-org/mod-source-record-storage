buildMvn {
  publishModDescriptor = 'yes'
  mvnDeploy = 'yes'
  publishAPI = 'yes'
  runLintRamlCop = 'yes'
  doKubeDeploy = true

  doDocker = {
    buildJavaDocker {
      publishMaster = 'yes'
      healthChk = 'yes'
      healthChkCmd = 'curl -sS --fail -o /dev/null  http://localhost:8081/apidocs/ || exit 1'
    }
  }
}

