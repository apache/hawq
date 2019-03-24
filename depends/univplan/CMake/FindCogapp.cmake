# locate cogapp and generate source code from template
# 
# find_package(Cogapp REQUIRED)
#
# COGAPP_GENERATE (public function)
#   GENERATED_CODE = Variable to define with generated source files.
#   TEMPLATE  = Template used to generate source files.
#
# NOTE: The COGAPP_GENERATE macro & add_executable() or add_library()
#       calls only work properly within the same directory.
#

find_package(PythonInterp REQUIRED)

function(COGAPP_GENERATE GENERATED_CODE)
  if(NOT ARGN)
    message(SEND_ERROR "Error: COGAPP_GENERATE() called without any template files")
    return()
  endif()

  set(${GENERATED_CODE})
  foreach(FIL ${ARGN})
    get_filename_component(ABS_FIL ${FIL} ABSOLUTE)
    file(RELATIVE_PATH FIL_REL ${CMAKE_SOURCE_DIR} ${ABS_FIL})
    
    get_filename_component(FIL_DIR ${CMAKE_BINARY_DIR}/codegen/${FIL_REL} DIRECTORY)
    file(MAKE_DIRECTORY ${FIL_DIR})
    
    get_filename_component(FIL_WE "${CMAKE_BINARY_DIR}/codegen/${FIL_REL}" NAME_WE)
    get_filename_component(FIL_EXT "${CMAKE_BINARY_DIR}/codegen/${FIL_REL}" EXT)
    
    set(FIL_OUT "${FIL_DIR}/${FIL_WE}.cg${FIL_EXT}")
    list(APPEND ${GENERATED_CODE} ${FIL_OUT})
    
    if(NOT EXISTS ${ABS_FIL})
        MESSAGE(FATAL_ERROR "file ${ABS_FIL} does not exist")
    endif()

    add_custom_command(
      OUTPUT ${FIL_OUT}
      COMMAND  ${PYTHON_EXECUTABLE}
      ARGS -m cogapp -d -o ${FIL_OUT} ${ABS_FIL}
      DEPENDS ${ABS_FIL}
      COMMENT "Running cog on ${FIL}"
      VERBATIM )
  endforeach()

  set_source_files_properties(${${GENERATED_CODE}} PROPERTIES GENERATED TRUE)
  set(${GENERATED_CODE} ${${GENERATED_CODE}} PARENT_SCOPE)
endfunction()