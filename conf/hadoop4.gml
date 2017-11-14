graph [
    label "hadoop_4"
    node [
        id          0
        label       "n1"
        longitude   1
        Latitude    2
    ]
    node [
        id          1
        label       "n2"
        longitude   6
        Latitude    1
    ]
    node [
        id          1
        label       "n3"
        longitude   7
        Latitude    1
    ]
    edge [
        source      0
        target      1
        bandwidth   2
    ]
    edge [
        source      1
        target      2
        bandwidth   2
    ]
    edge [
        source      0
        target      2
        bandwidth   2
    ]
]
