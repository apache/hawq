cwrapper.o: cwrapper.cpp cwrapper.h
	$(MAKE) -C format liborc_format.so
	g++ --shared -o cwrapper.o cwrapper.cpp -Lformat -lorc_format 

clean distclean:
	$(MAKE) -C format $@ || true;
	rm -f ./cwrapper.o