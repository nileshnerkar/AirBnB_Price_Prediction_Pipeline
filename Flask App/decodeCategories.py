import pandas as pd
def decode(field):
    lookup =[ {
        'Aparthotel': 0,
        'Apartment': 1,
        'Barn': 2,
        'Bed and breakfast': 3,
        'Boat': 4,
        'Boutique hotel': 5,
        'Bungalow': 6,
        'Bus': 7,
        'Cabin': 8,
        'Camper/RV': 9,
        'Casa particular (Cuba)': 10,
        'Castle': 11,
        'Cave': 12,
        'Condominium': 13,
        'Cottage': 14,
        'Dome house': 15,
        'Dorm': 16,
        'Earth house': 17,
        'Farm stay': 18,
        'Guest suite': 19,
        'Guesthouse': 20,
        'Hostel': 21,
        'Hotel': 22,
        'House': 23,
        'Houseboat': 24,
        'Island': 25,
        'Loft': 26,
        'Other': 27,
        'Resort': 28,
        'Serviced apartment': 29,
        'Tent': 30,
        'Tiny house': 31,
        'Townhouse': 32,
        'Villa': 33,
        'Yurt': 34
    },
    {
        'Bronx': 0,
        'Brooklyn': 1,
        'Manhattan': 2,
        'Queens': 3,
        'Staten Island': 4
    },
    {
        'Airbed': 0,
        'Couch': 1,
        'Futon': 2,
        'Pull-out Sofa': 3,
        'Real Bed': 4
    },
    {
        'flexible': 0,
        'moderate': 1,
        'strict': 2,
        'strict_14_with_grace_period': 3,
        'super_strict_30': 4,
        'super_strict_60': 5
    }
]

    for ele in lookup:
        if field in ele.keys():
            return ele[field]