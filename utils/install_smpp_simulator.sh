mkdir -p smppsim && \
cd smppsim && \
wget http://www.seleniumsoftware.com/downloads/SMPPSim.tar.gz && \
tar zxf SMPPSim.tar.gz && \
cd SMPPSim && \
sed -i.original -e s/HTTP_PORT=88/HTTP_PORT=8080/ conf/smppsim.props && \
chmod +x startsmppsim.sh && \
cd ../../