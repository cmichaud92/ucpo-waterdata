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
    "CAMC2",   # Colorado River - Cameo, CO *usgs 09095500
    "GJNC2",   # Gunnison River - Grand Junction, CO *usgs 09152500
    "DOLU1",   # Dolores River - Cisco, UT *usgs 09180000
    "GRVU1",   # Green River - Green River, UT *usgs 09315000
    "WATU1",   # White River - Watson, UT *usgs 09306500
    "DURU1",   # Duchesne River - Randlett, UT *usgs 09302000
    "YDLC2",   # Yampa River - Deerlodge Park, CO *usgs 09260050
    "GLDA3",   # Lake Powell, AZ (Colorado River) *usgs 09379900
    "GRNU1",   # Flaming Gorge Reservoir, UT (Green River) *usgs 09106485
    "RURC2",   # Ruedi Reservoir, CO (Frying Pan River) *usgs 09106150
    "WORC2",   # Wolford Mountain Reservoir, CO (Muddy Creek) *usgs 09041395
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
        '09180000', '09328960', '09147022', '09041395', '09379900', '913', '914',
        '915', '917', '919', '928', '1999', '2000', '2002', '2005', 'CAMC2',
        'GJNC2', 'DOLU1', 'GRVU1', 'WATU1', 'DURU1', 'YDLC2', 'GLDA3', 'GRNU1',
        'RURC2', 'WORC2', 'WFDC2', 'GBYC2', 'WCRC2', 'GRMC2', 'CLSC2', 'MPSC2',
        'BMDC2'
        ],
    "hydro_area_cd": [
        'GU', 'CO', 'CO', 'CO', 'CO', 'WH', 'YA', 'YA', 'LS', 'GR',
        'GR', 'DU', 'DO', 'CO', 'RIDGE', 'WOLF', 'POWELL', 'BLM', 'MORR',
        'CRY', 'FLA', 'POWELL', 'STAR', 'GRA', 'GRE', 'RDI', 'WIL', 'CO',
        'CO', 'DO', 'GR', 'WH', 'DU', 'YA', 'POWELL', 'FLA', 'RUDI',
        'WOLF', 'WIL', 'GRA', 'WIL', 'GRE', 'CRY', 'MORR', 'BLM'
    ],
    "hydro_area_nm": [
        'Gunnison River', 'Colorado River', 'Colorado River', 'Colorado River',
        'Colorado River', 'White River', 'Yampa River', 'Yampa River',
        'Little Snake River', 'Green River', 'Green River', 'Duchesne River',
        'Dolores River', 'Colorado River', 'Ridgeway Reservoir',
        'Wolford Mountain Reservoir', 'Lake Powell', 'Blue Mesa Reservoir',
        'Morrow Point Reservoir', 'Crystal Reservoir', 'Flaming Gorge Reservoir',
        'Lake Powell', 'Starvation Reservoir', 'Granby Reservoir',
        'Green Mountain Reservoir', 'Ruedi Reservoir', 'Williams Fork Reservoir',
        'Colorado River', 'Gunnison River', 'Dolores River', 'Green River',
        'White River', 'Duchesne River', 'Yampa River', 'Lake Powell',
        'Flaming Gorge Reservoir', 'Ruedi Reservoir', 'Wolford Mountain Reservoir',
        'Williams Fork Reservoir', 'Granby Reservoir', 'Willows Creek Reservoir',
        'Green Mountain Reservoir', 'Crystal Reservoir', 'Morrow Point Reservoir',
        'Blue Mesa Reservoir'
    ],
})

cbrfc_sites_df = pd.DataFrame({
    'site_cd': cbrfc_sites,
    'site_nm': [
        'Colorado River - Cameo, CO', 'Gunnison River - Grand Junction, CO',
        'Dolores River - Cisco, UT', 'Green River - Green River, UT',
        'White River - Watson, UT', 'Duchesne River - Randlett, UT',
        'Yampa River - Deerlodge Park, CO', 'Lake Powell, AZ (Colorado River)',
        'Flaming Gorge Reservoir, UT (Green River)',
        'Ruedi Reservoir, CO (Frying Pan River)',
        'Wolford Mountain Reservoir, CO (Muddy Creek)',
        'Williams Fork Reservoir, CO (Williams Fork River)',
        'Granby Reservoir, CO (Colorado River)',
        'Willows Creek Reservoir, CO (Willow Creek)',
        'Green Mountain Reservoir, CO (Blue River)',
        'Crystal Reservoir, CO (Gunnison River)',
        'Morrow Point Reservoir, CO (Gunnison River)',
        'Blue Mesa Reservoir, CO (Gunnison River)'
    ],
    'site_dsc': [None] * len(cbrfc_sites),
    'agency_cd': ['CBRFC'] * len(cbrfc_sites),
    'agency_nm': ['Colorado Basin River Forecast Center'] * len(cbrfc_sites),
    'lat_dd': [
        39.2391463, 38.9833158, 38.797208, 38.9860831, 39.9788563,
        40.2102778, 40.4516339, 36.9366548, None, None,
        None, None, None, None, None, None, None, None
    ],
    'lon_dd': [
        -108.2661946, -108.4506446, -109.1951142, -110.1512475, -109.1787269,
        -109.7813889, -108.525101, -111.4840472, None, None,
        None, None, None, None, None, None, None, None
    ],
    'elev_m': [None] * len(cbrfc_sites),
    'site_type': ['Stream'] * 7 + ['Reservoir'] * 11,
    'hydro_area_cd': [
        'CO', 'GU', 'DO', 'GR', 'WH', 'DU', 'YA', 'POWELL', 'FLA', 'RUDI',
        'WOLF', 'WIL', 'GRA', 'WIL', 'GRE', 'CRY', 'MORR', 'BLM'
    ],
    'hydro_area_nm': [
        'Colorado River', 'Gunnison River', 'Dolores River', 'Green River',
        'White River', 'Duchesne River', 'Yampa River', 'Lake Powell',
        'Flaming Gorge Reservoir', 'Ruedi Reservoir', 'Wolford Mountain Reservoir',
        'Williams Fork Reservoir', 'Granby Reservoir', 'Willows Creek Reservoir',
        'Green Mountain Reservoir', 'Crystal Reservoir', 'Morrow Point Reservoir',
        'Blue Mesa Reservoir'
    ],
    'source': [None] * len(cbrfc_sites),
})

params = pd.DataFrame({
    'parameter_cd': [
        '00060',  # Discharge, cubic feet per second
        '00010',  # Temperature, water, degrees Celsius
        '62614',  # Reservoir surface elevation, feet
        '15',     # Bank storage, acre-feet
        '29',      # Inflow, cubic feet per second
        '42',      # Total release, cubic feet per second
        '49',      # Pool elevation, feet
        '17',      # Storage, acre-feet
        '47',      # delta storage, acre-feet
        '25',      # Evaporation, acre-feet
        '30',      # Inflow volume, acre-feet
        '31',      # Side inflow, cubic feet per second
        '32',      # Side inflow volume, acre-feet
        '33',      # Unregulated inflow, cubic feet per second
        '34',      # Unregulated inflow volume, acre-feet
        '1197',    # Bypass release, cubic feet per second
        '1198',    # Bypass release volume, acre-feet
        '39',      # Power release, cubic feet per second
        '40',      # Power release volume, acre-feet
        '43',      # Release volume, acre-feet
        '46',      # Spillway release, cubic feet per second
        '89'      # Area, acres
    ],
    'parameter_nm': [
        'Discharge',
        'Water temperature',
        'Reservoir surface elevation',
        'Bank storage',
        'Inflow',
        'Total release',
        'Pool elevation',
        'Storage',
        'Delta storage',
        'Evaporation',
        'Inflow volume',
        'Side inflow',
        'Side inflow volume',
        'Unregulated inflow',
        'Unregulated inflow volume',
        'Bypass release',
        'Bypass release volume',
        'Power release',
        'Power release volume',
        'Release volume',
        'Spillway release',
        'Area'
    ],
    'parameter_dsc': [None] * 22,
    'unit_nm': [
        'Cubic feet per second',
        'Degrees Celsius',
        'Feet',
        'Acre-feet',
        'Cubic feet per second',
        'Cubic feet per second',
        'Feet',
        'Acre-feet',
        'Acre-feet',
        'Acre-feet',
        'Acre-feet',
        'Cubic feet per second',
        'Acre-feet',
        'Cubic feet per second',
        'Acre-feet',
        'Cubic feet per second',
        'Acre-feet',
        'Cubic feet per second',
        'Acre-feet',
        'Acre-feet',
        'Cubic feet per second',
        'Acres'
    ],
    'unit_cd': [
        'cfs',  # Flow rate
        'C',    # Temperature
        'ft',   # Elevation
        'af',   # Volume
        'cfs',  # Flow rate
        'cfs',  # Flow rate
        'ft',   # Elevation
        'af',   # Volume
        'af',   # Volume
        'af',   # Volume
        'af',   # Volume
        'cfs',  # Flow rate
        'af',   # Volume
        'cfs',  # Flow rate
        'af',   # Volume
        'cfs',  # Flow rate
        'af',   # Volume
        'cfs',  # Flow rate
        'af',   # Volume
        'af',   # Volume
        'cfs',  # Flow rate
        'ac'   # Area
    ],
})
