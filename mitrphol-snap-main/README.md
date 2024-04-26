# sen1-snap-mitrphol
 
Docker image for gdal and Sentinel Application Platform (SNAP) toolbox

# Sentinel Application Platform (SNAP) toolbox:

- Downloaded and installed from [SNAP All toolboxs for Unix 64-bit](https://step.esa.int/main/download/snap-download/)

See [Dockerfile](https://github.com/Thanut-Junnim26/sen1-processing/blob/main/mitrphol-snap-main/Dockerfile)

# Usage example
``` bash
    sudo docker build --tag sen1-snap .

    sudo docker run -it --rm --name sen1-snap -v $(pwd):/root/sentinel_process sen1-snap:latest 
    
    # background processing 
    sudo docker run -it -d --name sen1-snap -v $(pwd)/workspace/rawdata:/root/sentinel_process/workspace/rawdata sen1-snap:latest
    
    # resume
    docker exec -it gdal-snap:latest bash
```
# Note

``` bash
    rm  ~/.docker/config.json
```

