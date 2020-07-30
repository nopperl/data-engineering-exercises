./gradlew sync
./gradlew customFatJar
hadoop jar build/libs/ChiSquare-exec-1.0.jar /scratch/amazon-reviews/full/reviews_devset.json /scratch/ChiSquareTest
