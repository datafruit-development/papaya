once you are cd'd into the plugin directory, you can build the plugin using sbt:

```
brew install sbt
sbt package
```

run the spark job using something like

```
spark-submit \
  --jars ../plugin/target/scala-2.12/test-plugin_2.12-0.1.jar \
  --conf spark.plugins=test.TestPlugin \
  main.py
```
