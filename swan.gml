graph [
    label "paper"
    node [
        id          0
        label       "HK"
        Longitude   1
        Latitude    1
    ]
    node [
        id          1
        label       "LA"
        Longitude   1
        Latitude    1
    ]
    node [
        id          2
        label       "NY"
        Longitude   1
        Latitude    1
    ]
    node [
        id          3
        label       "FL"
        Longitude   1
        Latitude    1
    ]
    node [
        id          4
        label       "BA"
        Longitude   1
        Latitude    1
    ]
    edge [
        source      0
        target      1
        bandwidth   256
    ]
    edge [
        source      0
        target      2
        bandwidth   256
    ]
    edge [
        source      1
        target      2
        bandwidth   256
    ]
    edge [
        source      1
        target      3
        bandwidth   256
    ]
    edge [
        source      2
        target      3
        bandwidth   256
    ]
    edge [
        source      2
        target      4
        bandwidth   256
    ]
    edge [
        source      3
        target      4
        bandwidth   256
    ]
]
