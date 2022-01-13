clean:
	cd datacleaning && mvn clean && cd ../wordcount && mvn clean && cd ../hashtagbyuser && mvn clean
	
compile:
	make clean && cd datacleaning && mvn compile && cd ../wordcount && mvn compile && cd ../hashtagbyuser && mvn compile
	
package:
	make compile && cd datacleaning && mvn package && cd ../wordcount && mvn package && cd ../hashtagbyuser && mvn package
	
build:
	mkdir -p install && make package && cp datacleaning/target/Projet-Tweeter-1.0.jar install/ && cp wordcount/target/Projet-Tweeter-HashtagCount-1.0.jar install/ && cp hashtagbyuser/target/Projet-Tweeter-HashtagByUser-1.0.jar install/

send:
	make build && scp -r install dpereira004@ssh.enseirb-matmeca.fr:.
