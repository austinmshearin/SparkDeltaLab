@ECHO OFF
set /p containers=Delete all stopped containers? (y or n): 
if %containers%==y docker container prune -f
set /p images=Delete all images not associated with running containers? (y or n): 
if %images%==y docker image prune -a -f
set /p volumes=Delete all volumes not associated with running containers? (y or n): 
if %volumes%==y docker volume prune -a -f
set /p builds=Delete all build steps? (y or n): 
if %builds%==y docker builder prune -a -f
set /p close=Press enter to close
