cd Hidoop
gradle clean;gradle jar
rm ./Hidoop.jar
mv build/libs/Hidoop.jar ./
cd ..

cd client
gradle clean;gradle jar
rm ./Client.jar
mv build/libs/Client.jar ./
cd ..


cd wordcount
gradle clean;gradle jar
mv build/libs/Job.jar ./
cd ..

cd wordmedian
gradle clean;gradle jar
mv build/libs/Job.jar ./
cd ..

cd A2
gradle clean;gradle jar
mv build/libs/Job.jar ./
cd ..

cd A5
gradle clean;gradle jar
mv build/libs/Job.jar ./
cd ..


