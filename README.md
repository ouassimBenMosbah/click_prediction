# click_prediction
Predict user clicks on ads based on a dataset of real world data

## Dependencies
* You should install ```sbt``` [Installation guide](http://www.scala-sbt.org/download.html)
* spark-submit should be in your environnement path.

## Usage
On the root of the project you should run:

```
./run.sh path/to/train/data.json path/to/test/data.json
```

The script wil execute those 2 commands.
```
sbt package
spark-submit --class "PredictClick" --master local[2] target/scala-2.11/click_prediction_2.11-1.0-SNAPSHOT.jar path/train/data.json path/test/data.json
```
The result should be contained in 'result_predictions'.
nb: Be aware that you don't already have a directory called 'result_predictions'.

## Example
For example you can put train.json and a test.json in the root directory of this project and run:

```
./run.sh data-students.json test.json
```
