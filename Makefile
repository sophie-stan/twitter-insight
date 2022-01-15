INSTALL_DIR=install
ID=sstan
DATA_CLEAN=datacleaning
TAR_FILE=target/*jar

# Global packaging

clean:
	cd $(DATA_CLEAN) && mvn clean && cd ../wordcount &&\
mvn clean && cd ../hashtagbyuser &&\
mvn clean && cd ../topk && mvn clean

package:
	make clean && cd $(DATA_CLEAN) && mvn package &&\
cd ../wordcount && mvn package &&\
cd ../hashtagbyuser && mvn package &&\
cd ../topk && mvn package

build:
	mkdir -p $(INSTALL_DIR) && make package &&\
cp $(DATA_CLEAN)/$(TAR_FILE) $(INSTALL_DIR) &&\
cp wordcount/$(TAR_FILE) $(INSTALL_DIR) &&\
cp hashtagbyuser/$(TAR_FILE) $(INSTALL_DIR) &&\
cp topk/$(TAR_FILE) $(INSTALL_DIR)

send:
	make build && scp -r $(INSTALL_DIR) $(ID)@ssh.enseirb-matmeca.fr:.


# Individual packaging
.PHONY: datacleaning wordcount topk hashtagbyuser

datacleaning:
	cd $@ && mvn clean package && cp $(TAR_FILE) ../$(INSTALL_DIR)

wordcount:
	cd $@ && mvn clean package && cp $(TAR_FILE) ../$(INSTALL_DIR)

topk:
	cd $@ && mvn clean package && cp $(TAR_FILE) ../$(INSTALL_DIR)

hashtagbyuser:
	cd $@ && mvn clean package && cp $(TAR_FILE) ../$(INSTALL_DIR)

export:
	scp -r $(INSTALL_DIR) $(ID)@ssh.enseirb-matmeca.fr:.
