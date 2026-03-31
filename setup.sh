#!/bin/bash

set -e  # Exit on error

echo "🚀 Starting Dev Environment Setup..."

#######################################
# 0. Prerequisites
#######################################
echo "🔧 Installing Xcode CLI tools..."
xcode-select --install || true

#######################################
# 1. Homebrew (Apple Silicon)
#######################################
if ! command -v brew &>/dev/null; then
  echo "🍺 Installing Homebrew..."
  /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
fi

echo "🔄 Updating Homebrew..."
brew update

#######################################
# 2. Common Tools (Core + Python)
#######################################
echo "📦 Installing common tools..."

brew install \
  git \
  wget \
  curl \
  gradle \
  maven \
  kubectl \
  helm \
  azure-cli \
  k9s \
  podman \
  jenv \
  pyenv

#######################################
# 3. GUI Apps via Cask
#######################################
echo "🖥 Installing GUI apps..."

brew install --cask \
  dbeaver-community \
  pgadmin4 \
  postman \
  intellij-idea-ce \
  pycharm-ce

#######################################
# 4. Telepresence (manual tap)
#######################################
echo "🌐 Installing Telepresence..."
brew install telepresenceio/telepresence/telepresence

#######################################
# 5. Java Setup (Core)
#######################################
echo "☕ Installing JDKs..."
brew install openjdk@17 openjdk@21

echo "🔗 Linking JDKs..."
sudo ln -sfn /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-17.jdk || true
sudo ln -sfn /opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-21.jdk || true

echo "⚙️ Configuring jenv..."
eval "$(jenv init -)"
jenv add /Library/Java/JavaVirtualMachines/openjdk-17.jdk/Contents/Home || true
jenv add /Library/Java/JavaVirtualMachines/openjdk-21.jdk/Contents/Home || true

#######################################
# 6. Python Setup
#######################################
echo "🐍 Installing Python via pyenv..."

export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"

pyenv install -s 3.11.7
pyenv global 3.11.7

echo "📦 Installing pip tools..."
pip install --upgrade pip setuptools virtualenv

#######################################
# 7. M2 Directory Setup
#######################################
echo "📁 Creating .m2 directory..."
mkdir -p ~/.m2

#######################################
# 8. Git Config (Optional)
#######################################
echo "🔧 Setting up Git..."
git config --global init.defaultBranch main

#######################################
# 9. Certificates Placeholder
#######################################
echo "🔐 NOTE: Add Digital Platform Certificates manually:"
echo "   → Keychain Access → System → Import certs"

#######################################
# 10. Artifactory Config Placeholder
#######################################
echo "📦 NOTE: Configure Artifactory manually in ~/.m2/settings.xml"

#######################################
# 11. Clone Core Repositories
#######################################
CODEBASE_DIR="$HOME/Documents/codebase"

echo "📂 Using codebase directory: $CODEBASE_DIR"
mkdir -p "$CODEBASE_DIR"
cd "$CODEBASE_DIR"

# Example repos (replace with your org repos)
REPOS=(
  "git@github.com:your-org/core-service.git"
  "git@github.com:your-org/common-lib.git"
)

for repo in "${REPOS[@]}"; do
  repo_name=$(basename "$repo" .git)
  if [ ! -d "$repo_name" ]; then
    echo "📥 Cloning $repo..."
    git clone "$repo"
  else
    echo "✔️ $repo_name already exists"
  fi
done

#######################################
# 12. Final Cleanup
#######################################
echo "🧹 Cleaning up..."
brew cleanup

#######################################
# Done
#######################################
echo "✅ Setup Completed Successfully!"
echo "👉 Restart terminal or run: source ~/.zshrc"
