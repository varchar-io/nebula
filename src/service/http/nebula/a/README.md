# Web Editors
For both editors, we import (copy) the only supported source files. Both supports lots of language modes and different themes. 


## Editor 1
This is product of ACE - https://ace.c9.io/
The project is hosted at github: https://github.com/ajaxorg/ace
Copy its build result here so that we can use it for code editor.
Here are the path mappings for the copy:
cp ~/ace/build/src-noconflict/ace.js ../src/service/http/nebula/a/ace.js
cp ~/ace/build/src-min-noconflict/ace.js ../src/service/http/nebula/a/ace.min.js
cp ~/ace/build/src-noconflict/mode-javascript.js ../src/service/http/nebula/a/mode-javascript.js
cp ~/ace/build/src-min-noconflict/mode-javascript.js ../src/service/http/nebula/a/mode-javascript.min.js
cp ~/ace/build/src-min-noconflict/theme-cobalt.js ../src/service/http/nebula/a/theme-cobalt.min.js
cp ~/ace/build/src-noconflict/theme-cobalt.js ../src/service/http/nebula/a/theme-cobalt.js

## Editor 2
Code Mirror is another choice, looks like it is not as rich as ace but simpler.
Hence its size seems much smaller (~50% of ACE), the project can be found here https://github.com/codemirror/CodeMirror
This editor is placed in folder m/