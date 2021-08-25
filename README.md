# MaSEC: Discovering Anchorages and Co-movement Patterns on Streaming Vessel Trajectories

A Python3 implementation of the MaSEC framework.



## Installation
Install the dependencies included with the following command
``` Python
pip install -r requirements.txt
```


## Usage
To use MaSEC at first run the ```MaSEC.py``` script using the following command
``` Python
python MaSEC.py
```

which will initialize Zookeeper and Apache Kafka and create the Topics needed for the data stream and its results. Afterwards, in order to instantiate the web application (```monitor```) for the visualization, run the following command
``` Python
python -m bokeh serve --show ./monitor
```

Adjusting the parameters of MaSEC is possible via the ```lib/kafka_config_c_p_v01.py``` file. More information regarding the parameters and their allowed values (and types) can be found at ```lib/README.md```.




## Contributors
Andreas Tritsarolis, Yannis Kontoulis, Nikos Pelekis, Yannis Theodoridis; Data Science Lab., University of Piraeus


## Acknowledgement
This work was partially supported by projects i4Sea (grant T1EDK-03268) and VesselAI (grant agreement No 957237), which have received funding by the European Regional Development Fund of the EU and Greek national funds (through the Operational Program Competitiveness, Entrepreneurship and Innovation, under the call Research-Create-Innovate) and the EU Horizon 2020 R&I Programme, respectively.


## Citation info
If you use MaSEC in your project, we would appreciate citations to the following paper:

> Andreas Tritsarolis, Yannis Kontoulis, Nikos Pelekis, Yannis Theodoridis. 2021. MaSEC: Discovering Anchorages and Co-movement Patterns on Streaming Vessel Trajectories. In: SSTD Conference. ACM Press.
