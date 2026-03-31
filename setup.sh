#!/bin/bash

set +e  # DO NOT exit on error (important)

echo "========================================"
echo "🚀 DEV SETUP STARTED"
echo "========================================"

#######################################
# Helper Functions
#######################################

log() {
  echo -e "\n🔹 $1"
}

success() {
  echo "✅ $1"
}

fail() {
  echo "❌ $1"
}

retry() {
  local n=0
  local max=3
  local delay=5

  until [ $n -ge $max ]
  do
    "$@" && break
    n=$((n+1))
    echo "⚠️ Retry $n/$max for: $*"
    sleep $delay
  done

  if [ $n -ge $max ]; then
    echo "❌ Failed after retries: $*"
  fi
}

#######################################
# 0. Kill Brew Locks
#######################################
log "Cleaning brew locks"

pkill -f brew 2>/dev/null
rm -rf ~/Library/Caches/Homebrew/downloads/*.incomplete 2>/dev/null
rm -rf ~/Library/Caches/Homebrew/locks 2>/dev/null

success "Brew locks cleaned"

#######################################
# 1. Xcode CLI
#######################################
log "Installing Xcode CLI"

xcode-select --install 2>/dev/null
success "Xcode CLI triggered (ignore if already installed)"

#######################################
# 2. Homebrew
#######################################
log "Checking Homebrew"

if ! command -v brew &>/dev/null; then
  log "Installing Homebrew"
  retry /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
fi

eval "$(/opt/homebrew/bin/brew shellenv)"
retry brew update

success "Homebrew ready"

#######################################
# 3. Package Install (Batch + Retry)
#######################################
install_pkg() {
  for pkg in "$@"; do
    log "Installing $pkg"
    retry brew install "$pkg"
  done
}

install_pkg git wget curl
install_pkg gradle
install_pkg kubectl helm
install_pkg azure-cli k9s
install_pkg podman jenv pyenv

#######################################
# 4. Maven (Fail-safe)
#######################################
log "Installing Maven"

retry brew install maven

if ! command -v mvn &>/dev/null; then
  fail "Brew Maven failed → fallback install"

  MAVEN_VERSION=3.9.9
  retry curl -L -o apache-maven.tar.gz https://archive.apache.org/dist/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz

  tar -xzf apache-maven.tar.gz
  sudo mv apache-maven-$MAVEN_VERSION /opt/maven

  echo 'export MAVEN_HOME=/opt/maven' >> ~/.zshrc
  echo 'export PATH=$MAVEN_HOME/bin:$PATH' >> ~/.zshrc

  export MAVEN_HOME=/opt/maven
  export PATH=$MAVEN_HOME/bin:$PATH
fi

success "Maven setup done"

#######################################
# 5. GUI Apps
#######################################
log "Installing GUI apps"

retry brew install --cask dbeaver-community
retry brew install --cask pgadmin4
retry brew install --cask postman
retry brew install --cask intellij-idea-ce
retry brew install --cask pycharm-ce

#######################################
# 6. Telepresence
#######################################
log "Installing Telepresence"

retry brew install telepresenceio/telepresence/telepresence

#######################################
# 7. Java Setup
#######################################
log "Installing Java"

retry brew install openjdk@17
retry brew install openjdk@21

sudo ln -sfn /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-17.jdk 2>/dev/null
sudo ln -sfn /opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-21.jdk 2>/dev/null

eval "$(jenv init -)"
jenv add /Library/Java/JavaVirtualMachines/openjdk-17.jdk/Contents/Home 2>/dev/null
jenv add /Library/Java/JavaVirtualMachines/openjdk-21.jdk/Contents/Home 2>/dev/null

#######################################
# 8. Python Setup
#######################################
log "Setting up Python"

export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"

retry pyenv install -s 3.11.7
pyenv global 3.11.7

retry pip install --upgrade pip setuptools virtualenv

#######################################
# 9. M2 Directory
#######################################
log "Creating .m2 directory"

mkdir -p ~/.m2

#######################################
# 10. Codebase Setup
#######################################
log "Setting up codebase"

CODEBASE_DIR="$HOME/Documents/codebase"
mkdir -p "$CODEBASE_DIR"
cd "$CODEBASE_DIR"

#######################################
# 11. Final Cleanup
#######################################
log "Cleanup"

brew cleanup

#######################################
# 12. SANITY CHECKS 🔍
#######################################
echo ""
echo "========================================"
echo "🔍 SANITY CHECK RESULTS"
echo "========================================"

check() {
  if command -v $1 &>/dev/null; then
    echo "✅ $1 installed → $($1 --version 2>/dev/null | head -n 1)"
  else
    echo "❌ $1 NOT installed"
  fi
}

check git
check java
check mvn
check gradle
check python
check kubectl
check helm
check az

echo "========================================"
echo "🎉 SETUP COMPLETED (WITH FAIL-SAFE)"
echo "========================================"

echo "👉 Run: source ~/.zshrc"
