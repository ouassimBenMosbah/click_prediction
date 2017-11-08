if [ "$#" -ne 2 ]; then
  echo "You should define both paths"
  exit 1
fi

sbt package
spark-submit --class "PredictClick" --master local[2] target/scala-2.11/click_prediction_2.11-1.0-SNAPSHOT.jar $1 $2

