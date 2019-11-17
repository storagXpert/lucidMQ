cmake_minimum_required(VERSION 2.8.8)

project(lucidMQ)

IF(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
	SET(CMAKE_INSTALL_PREFIX ${CMAKE_BINARY_DIR} CACHE PATH "default binary path" FORCE)
ENDIF(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)

find_package(LibXml2 REQUIRED)
find_package(Threads REQUIRED)

include_directories(${LIBXML2_INCLUDE_DIR})
add_definitions(${LIBXML2_DEFINITIONS})

add_executable(lucidd util.c daemon.c initconf.c sender.c receiver.c xmlconf.c)
target_link_libraries(lucidd ${LIBXML2_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})

add_executable(lucidctl lucidctl.c)

add_library(lucidcl STATIC clientlib/lucidmq.c)
set_target_properties(lucidcl PROPERTIES PUBLIC_HEADER "clientlib/lucidmq.h")

add_executable(mqread clientlib/mqread.c)
target_link_libraries(mqread lucidcl ${CMAKE_THREAD_LIBS_INIT})

add_executable(mqwrite clientlib/mqwrite.c)
target_link_libraries(mqwrite lucidcl ${CMAKE_THREAD_LIBS_INIT})

install(TARGETS lucidd lucidctl lucidcl mqread mqwrite
	RUNTIME DESTINATION bin
	ARCHIVE DESTINATION lib
	PUBLIC_HEADER DESTINATION lib)

