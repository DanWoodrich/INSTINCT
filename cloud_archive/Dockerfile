FROM ubuntu:latest

RUN useradd docker \
	&& mkdir /home/docker \
	&& chown docker:docker /home/docker \
	&& addgroup docker staff
	
RUN apt-get update \
	&& apt-get install -y --no-install-recommends \
		software-properties-common \
        dirmngr \
        ed \
		less \
		locales \
		vim-tiny \
		wget \
		ca-certificates \
        && add-apt-repository --enable-source --yes "ppa:marutter/rrutter4.0" \
        && add-apt-repository --enable-source --yes "ppa:c2d4u.team/c2d4u4.0+"

RUN echo "en_US.UTF-8 UTF-8" >> /etc/locale.gen &&\
	locale-gen en_US.utf8 &&\
	/usr/sbin/update-locale LANG=en_US.UTF-8
	
ENV LC_ALL en_US.UTF-8
ENV LANG en_US.UTF-8

ENV DEBIAN_FRONTEND=noninteractive

ENV TZ UTC

ENV GCSFUSE_REPO=gcsfuse-bionic

RUN apt-get update &&\
	apt-get install -y --no-install-recommends apt-utils \
		curl \
		gnupg \
		littler \
		r-base r-base-dev r-recommended build-essential python3.6 python3-pip python3-setuptools python3-dev &&\
	echo "deb http://packages.cloud.google.com/apt $GCSFUSE_REPO main" | tee /etc/apt/sources.list.d/gcsfuse.list &&\
	curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - &&\
	apt-get update &&\
	apt-get remove libx11-dev &&\
	apt-get install -y libx11-dev &&\
	apt-get install -y libxml2-dev &&\
	apt-get install -y libglpk-dev &&\
	apt-get install -y libfftw3-dev &&\
	apt-get install -y libtiff5-dev &&\
	apt-get update &&\
	apt-get install gcsfuse -y &&\
	apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* &&\
	add-apt-repository --enable-source --yes "ppa:marutter/rrutter4.0" &&\ 
	add-apt-repository --enable-source --yes "ppa:c2d4u.team/c2d4u4.0+" &&\
	ln -s /usr/lib/R/site-library/littler/examples/install.r /usr/local/bin/install.r &&\
 	ln -s /usr/lib/R/site-library/littler/examples/install2.r /usr/local/bin/install2.r &&\
 	ln -s /usr/lib/R/site-library/littler/examples/installGithub.r /usr/local/bin/installGithub.r &&\
 	ln -s /usr/lib/R/site-library/littler/examples/testInstalled.r /usr/local/bin/testInstalled.r &&\
 	install.r docopt &&\
 	rm -rf /tmp/downloaded_packages/ /tmp/*.rds &&\
 	rm -rf /var/lib/apt/lists/* 


WORKDIR /app

COPY . /app

RUN mkdir /app/Cache &&\
	mkdir /app/Outputs &&\
	mkdir /app/mnt
	
RUN pip3 install -r /app/etc/requirements.txt && Rscript /app/bin/Installe.R 

ENV FG="${FG}"

WORKDIR /app/bin

CMD ["sh","-c","gcsfuse -o allow_other --implicit-dirs --file-mode=777 --dir-mode=777 instinct_test /app/mnt/ && gcsfuse -o allow_other --implicit-dirs --file-mode=777 --dir-mode=777 instinct_cache /app/Cache/ && gcsfuse -o allow_other --implicit-dirs --file-mode=777 --dir-mode=777 instinct_outputs /app/Outputs/ && python3 runFullNovel.py $FG && fusermount -u /app/mnt/ && fusermount -u /app/Cache/ && fusermount -u /app/Outputs/ && python3 kill_vm.py"]


