# Kertish DFS File System Tool

```
Kertish-dfs (vXX.X.XXXX) usage: 

   kertish-dfs [options] command [arguments] parameters

options:
  --head-address   Points the end point of head node to work with. Default: localhost:4000
  --help           Prints this usage documentation
  --version        Prints release version

commands:
  mkdir   Create folders.
  ls      List files and folders.
  cp      Copy file or folder.
  mv      Move file or folder.
  rm      Remove files and/or folders.
  sh      Enter shell mode of fs-tool.
```

### Shell Commands

```
      __ _  ____  ____  ____  __  ____  _  _       ____  ____  ____                                                                    
     (  / )(  __)(  _ \(_  _)(  )/ ___)/ )( \     (    \(  __)/ ___)                                                                   
      )  (  ) _)  )   /  )(   )( \___ \) __ (      ) D ( ) _) \___ \                                                                   
     (__\_)(____)(__\_) (__) (__)(____/\_)(_/     (____/(__)  (____/                                                                   
FileSystem Shell vXX.X.XXXX, Visit: https://github.com/freakmaxi/kertish-dfs                                                           
processing... ok.                                                                                                                      
                                                                                                                                       
(/)                                                                                                                                    
 ➜ help                                                                                                                                
available commands:                                                                                                                    
  cd      Change directory.                                                                                                            
  mkdir   Create folders.                                                                                                              
  ls      List files and folders.                                                                                                      
  cp      Copy file or folder.                                                                                                         
  mv      Move file or folder.                                                                                                         
  rm      Remove files and/or folders.                                                                                                 
  help    Show this screen.                                                                                                            
          Ex: help [command] or help shortcuts                                                                                         
  exit    Exit from shell.                                                                                                                                
```

#### Shell Shortcuts

```text
Escape    :   Clear/Cancel line
Up        :   Older history
Down      :   Newer history
Home      :   Move cursor to line head
End       :   Move cursor to line end
PageUp    :   Scroll up
PageDown  :   Scroll down
Ctrl+T    :   Top of the terminal
Ctrl+B    :   Bottom of the terminal
Ctrl+Y    :   Page up in the terminal
Ctrl+V    :   Page down in the terminal
Ctrl+W    :   Remove previous word
Backspace :   Remove previous char
Left      :   Move cursor to previous char
Alt+Left  :   Jump to previous word
Right     :   Move cursor to next char
Alt+Right :   Jump to next word
Ctrl+R    :   Refresh terminal cache
Tab       :   Complete path
Enter     :   Execute command
```