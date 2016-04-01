cd sorting
gradle jar
rm ../Job.jar
mv build/libs/Job.jar ../
cd ..

cd client
gradle jar
rm ../Client.jar
mv build/libs/Client.jar ../
cd ..
