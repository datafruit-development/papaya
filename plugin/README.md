# Dev reference

quick guide if you are a developer making changes to the spark plugin itself (for macOS/Linux)


### proper dev setup
make sure you are using java 17 the LATEST
```
brew install openjdk@17
sudo ln -sfn /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-17.jdk
```

and add this to zshrc
```
export JAVA_HOME=$(/usr/libexec/java_home -v17)
export PATH="$JAVA_HOME/bin:$PATH"
```

and install sbt

```
brew install sbt
```

---

### building plugin jars
To build all versions
```
cd plugin
sbt +package
```

which will create the relevant jars under `papaya/plugin_jars/spark-3.x.x-scala-2.x.x.jar`
