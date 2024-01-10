cd /D "%~dp0"
call "%~dp0..\..\..\..\..\..\..\Scripts\activate" Pydro38
sphinx-build -b html "%~dp0" "%~dp0..\docbuild"
