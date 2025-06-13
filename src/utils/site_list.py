import pandas as pd

nwis_sites = [
    "09152500",   # Gunnison River Near Grand Junction, CO
    "09095500",   # Colorado River Near Cameo, CO
    "09106150",   # Colorado River Below Grand Valley Div NR Palisade, CO
    "09106485",   # Colorado River Above Gunnison River at Grand Junction, CO
    "09163500",   # Colorado River Near Colorado-utah State Line
    "09306500",   # White River Near Watson, Utah
    "09251000",   # Yampa River Near Maybell, CO
    "09260050",   # Yampa River at Deerlodge Park, CO
    "09260000",   # Little Snake River Near Lily, CO
    "09261000",   # Green River Near Jensen, UT
    "09315000",   # Green River at Green River, UT
    "09302000",   # Duchesne River Near Randlett, UT
    "09180000",   # Dolores River Near Cisco, UT
    "09328960",   # Colorado River at Gypsum Canyon Near Hite, UT
    "09147022",   # Ridgeway Reservoir Near Ridgway, CO
    "09041395",   # Wolford Mtn Reservoir Nr Kremmling, CO
    "09379900",   # Lake Powell at Glen Canyon Dam, AZ
]

bor_sites = [
    "913",    # Blue Mesa Reservoir, CO
    "914",    # Morrow Point Reservoir, CO
    "915",    # Crystal Reservoir, CO
    "917",    # Flaming Gorge Reservoir, UT
    "919",    # Lake Powell, AZ
    "928",    # Starvation Reservoir, UT
    "1999",   # Granby Reservoir, CO
    "2000",   # Green Mountain Reservoir, CO
    "2002",   # Ruedi Reservoir, CO
    "2005",   # Williams Fork Reservoir, CO
]

cbrfc_sites = [
    "CAMC2",   # Colorado River - Cameo, CO
    "GJNC2",   # Gunnison River - Grand Junction, CO
    "DOLU1",   # Dolores River - Cisco, UT
    "GRVU1",   # Green River - Green River, UT
    "WATU1",   # White River - Watson, UT
    "DURU1",   # Duchesne River - Randlett, UT
    "YDLC2",   # Yampa River - Deerlodge Park, CO
    "GLDA3",   # Lake Powell, AZ (Colorado River)
    "GRNU1",   # Flaming Gorge Reservoir, UT (Green River)
    "RURC2",   # Ruedi Reservoir, CO (Frying Pan River)
    "WORC2",   # Wolford Mountain Reservoir, CO (Muddy Creek)
    "WFDC2",   # Williams Fork Reservoir, CO (Williams Fork River)
    "GBYC2",   # Granby Reservoir, CO (Colorado River)
    "WCRC2",   # Willows Creek Reservoir, CO (Willow Creek)
    "GRMC2",   # Green Mountain Reservoir, CO (Blue River)
    "CLSC2",   # Crystal Reservoir, CO (Gunnison River)
    "MPSC2",   # Morrow Point Reservoir, CO (Gunnison River)
    "BMDC2",   # Blue Mesa Reservoir, CO (Gunnison River)
]

hydrologic_areas = pd.DataFrame({
    "site_cd": [
        '09152500', '09095500', '09106150', '09106485', '09163500', '09306500', 
        '09251000', '09260050', '09260000', '09261000', '09315000', '09302000', 
        '09180000', '09328960', '09147022', '09041395', '09379900'
        ],
    "hydro_area_cd": [
        'GU', 'CO', 'CO', 'CO', 'CO', 'WH', 'YA', 'YA', 'LS', 'GR',
        'GR', 'DU', 'DO', 'CO', 'RIDGE', 'WOLF', 'POWELL'
    ],
    "hydro_area_nm": [
        'Gunnison River', 'Colorado River', 'Colorado River', 'Colorado River',
        'Colorado River', 'White River', 'Yampa River', 'Yampa River',
        'Little Snake River', 'Green River', 'Green River', 'Duchesne River',
        'Dolores River', 'Colorado River', 'Ridgeway Reservoir',
        'Wolford Mountain Reservoir', 'Lake Powell'
    ],
})
