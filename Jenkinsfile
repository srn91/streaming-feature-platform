pipeline {
  agent any

  environment {
    PYTHON = "python3.12"
  }

  stages {
    stage('Install') {
      steps {
        sh '$PYTHON -m pip install -r requirements.txt'
      }
    }
    stage('Lint and test') {
      steps {
        sh 'ruff check src tests'
        sh 'pytest tests'
      }
    }
    stage('Container smoke') {
      steps {
        sh 'docker build -t streaming-feature-platform:ci .'
      }
    }
  }
}
