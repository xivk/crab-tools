#!/usr/bin/env python

# Import Psyco if available
try:
    import psyco
    psyco.full()
except ImportError:
    pass

from dbfpy.dbf import Dbf
from constants.extensions import CSV

import sys
import parser
import argparse
from elementtree.SimpleXMLWriter import XMLWriter
from lambert import Belgium1972LambertProjection

parser = argparse.ArgumentParser(description='Reads the AGIV CRAB database in DBF and converts this to .csv/.osm format.')
parser.add_argument('path', help='The path to the CRAB DBF files.')
parser.add_argument('--output-csv', default='crab.csv', help='The path to the output csv file.')
parser.add_argument('--filter-postcode', help='The postocde to filter on, will restrict data to this postcode only.', default='')
parser.add_argument('--write-postcodes', action='store_true', default=False)
parser.add_argument('--output-osm', default='crab.osm', help='The path to the output OSM XML file.')
args = parser.parse_args()

straatnm_dbf = args.path + 'straatnm.dbf'
huisnr_dbf = args.path + 'huisnr.dbf'
pkancode_dbf = args.path + 'pkancode.dbf'
gemnm_dbf = args.path + 'gemnm.dbf'
gem_dbf = args.path + 'gem.dbf'
tobjhnr_dbf = args.path + 'tobjhnr.dbf'
terrobj_dbf = args.path + 'terrobj.dbf'

do_terrobj = 1
do_tobjhnr = 1
do_huisnr = 1

postal_code = 0
if(len(args.filter-postcode) > 0):
    postal_code = int(args.filter-postcode)
    print 'Filtering on postalcode: ' + str(postal_code)

# parse & index pkancode
huisnr_dic = dict()
pkancode_set = set()

print 'Extracting pkancode'
db = Dbf()
db.openFile(pkancode_dbf, readOnly = 1)
record_count = db.recordCount()

for i in range(0, record_count):
    rec = db[i]
    
    if(i % (record_count / 50) is 0 and not i is 0):
        sys.stdout.write('.')
        sys.stdout.flush()
    
    huisnr_id = rec['HUISNRID']
    pkancode = rec['PKANCODE']

    if(pkancode == postal_code or postal_code is 0):
        huisnr_dic[huisnr_id] = dict()
        huisnr_dic[huisnr_id]['PKANCODE'] = pkancode
        pkancode_set.add(pkancode)

print ''

# parse & index tobjhnr
if(do_tobjhnr):
    print 'Extracting tobjhnr'
    terrobj_to_huirnr_id = dict()
    db = Dbf()
    db.openFile(tobjhnr_dbf, readOnly = 1)
    record_count = db.recordCount()

    for i in range(0, record_count):
        rec = db[i]
    
        if((i) % (record_count / 50) is 0 and not i is 0):
            sys.stdout.write('.')
            sys.stdout.flush()
    
        huisnr_id = rec['HUISNRID']
        if(huisnr_id in huisnr_dic):
            terrobj_to_huirnr_id[rec['TERROBJID']] = huisnr_id

    print ''

# parse & index terrobj
if(do_terrobj):
    print 'Extracting terrobj'
    db = Dbf()
    db.openFile(terrobj_dbf, readOnly = 1)
    record_count = db.recordCount()

    for i in range(0, record_count):
        rec = db[i]
    
        if((i) % (record_count / 50) is 0 and not i is 0):
            sys.stdout.write('.')
            sys.stdout.flush()
    
        terrobj_id = rec['ID']
        if(terrobj_id in terrobj_to_huirnr_id):
            huisnr_id = terrobj_to_huirnr_id[terrobj_id]
            huisnr_dic[huisnr_id]['X'] = rec['X']
            huisnr_dic[huisnr_id]['Y'] = rec['Y']
    print ''

# parse & index huisnr
if(do_huisnr):
    print 'Extracting huisnr'
    db = Dbf()
    db.openFile(huisnr_dbf, readOnly = 1)
    record_count = db.recordCount()

    for i in range(0, record_count):
        rec = db[i]
    
        if((i) % (record_count / 50) is 0 and not i is 0):
            sys.stdout.write('.')
            sys.stdout.flush()

        huisnr_id = rec['ID']
        if(huisnr_id in huisnr_dic):
            huisnr_dic[huisnr_id]['STRAATNMID'] = rec['STRAATNMID']
            huisnr_dic[huisnr_id]['HUISNR'] = rec['HUISNR']

    print ''

# parse & index straatnm
print 'Extracting straatnm:'
db = Dbf()
db.openFile(straatnm_dbf, readOnly = 1)
record_count = db.recordCount()

fields = [ 'STRAATNM', 'TAALCODE', 'NISGEMCODE', 'STRAATNM2', 'TAALCODE2' ]
straatnm_dic = parser.recordsD(db, 0, record_count, fields, 'ID')

db.close()
print ''

# index per ID and extract lanuages.
straatnm_lang_dic = dict()

for(straatnm_id, straatnm_fields) in straatnm_dic.items():
    straatnm_lang_dic[straatnm_id] = dict()
    straatnm_lang_dic[straatnm_id]['NISGEMCODE'] = straatnm_fields['NISGEMCODE']
    straatnm_lang_dic[straatnm_id]['NAME_NL'] = ''
    straatnm_lang_dic[straatnm_id]['NAME_FR'] = ''
    straatnm_lang_dic[straatnm_id]['NAME_DE'] = ''
    
    if(len(straatnm_fields['TAALCODE']) > 0):
        straatnm_lang_dic[straatnm_id]['NAME_' + straatnm_fields['TAALCODE'].upper()] = straatnm_fields['STRAATNM']
    if(len(straatnm_fields['TAALCODE2']) > 0):
        straatnm_lang_dic[straatnm_id]['NAME_' + straatnm_fields['TAALCODE2'].upper()] = straatnm_fields['STRAATNM2']

del straatnm_dic

for (huisnr_id, huisnr_fields) in huisnr_dic.items():
    straatnm_id = huisnr_fields['STRAATNMID']

    huisnr_fields['NISGEMCODE'] = straatnm_lang_dic[straatnm_id]['NISGEMCODE']
    huisnr_fields['STREET_NL'] = straatnm_lang_dic[straatnm_id]['NAME_NL']
    huisnr_fields['STREET_FR'] = straatnm_lang_dic[straatnm_id]['NAME_FR']
    huisnr_fields['STREET_DE'] = straatnm_lang_dic[straatnm_id]['NAME_DE']

# parse & index gem
print 'Extracting gem:'
db = Dbf()
db.openFile(gem_dbf, readOnly = 1)
record_count = db.recordCount()

fields = [ 'NISGEMCODE']
gem_dic = parser.recordsD(db, 0, record_count, fields, 'ID')

db.close()
print ''

# parse & index gemnm
print 'Extracting gemnm:'
db = Dbf()
db.openFile(gemnm_dbf, readOnly = 1)
record_count = db.recordCount()

fields = [ 'GEMID', 'TAALCODE', 'GEMNM' ]
gemnm_dic = parser.recordsD(db, 0, record_count, fields, 'ID')

db.close()
print ''

# index per NISGEMCODE and extract languages.
gemnm_lang_dic = dict()

for (gemnm_id, gemnm_fields) in gemnm_dic.items():
    gem_id = gemnm_fields['GEMID']
    niscode = gem_dic[gem_id]['NISGEMCODE']
    if(not niscode in gemnm_lang_dic):
        gemnm_lang_dic[niscode] = dict()
        gemnm_lang_dic[niscode]['NAME_NL'] = ''
        gemnm_lang_dic[niscode]['NAME_FR'] = ''
        gemnm_lang_dic[niscode]['NAME_DE'] = ''
    
    lang_field_name = 'NAME_' + gemnm_fields['TAALCODE'].upper()
    
    gemnm_lang_dic[niscode][lang_field_name] = gemnm_fields['GEMNM']

del gemnm_dic
del gem_dic

projection = Belgium1972LambertProjection()

for (huisnr_id, huisnr_fields) in huisnr_dic.items():
    niscode = huisnr_fields['NISGEMCODE']
        
    huisnr_fields['COMMUNE_NL'] = gemnm_lang_dic[niscode]['NAME_NL']
    huisnr_fields['COMMUNE_FR'] = gemnm_lang_dic[niscode]['NAME_FR']
    huisnr_fields['COMMUNE_DE'] = gemnm_lang_dic[niscode]['NAME_DE']

    # convert to lat/lon
    if('X' in huisnr_fields):
        coordinates = projection.to_wgs84(huisnr_fields['X'], huisnr_fields['Y'])
    
        huisnr_fields['LAT'] = coordinates[0]
        huisnr_fields['LON'] = coordinates[1]
    else:
        huisnr_fields['LAT'] = ''
        huisnr_fields['LON'] = ''

fields = [ 'COMMUNE_NL', 'COMMUNE_FR', 'COMMUNE_DE', 'PKANCODE', 'STREET_NL', 'STREET_FR', 'STREET_DE', 'HUISNR', 'LAT', 'LON']

output = open(args.output_csv, 'w')
if(len(args.output_osm) > 0):
    w = XMLWriter(args.OUTFILE)
    w.start("osm", {"generator": "crab-tools" + str(__version__), "version": API_VERSION, "upload": "false"})
rec_str = ''
for field in fields:
    rec_str += field + ','
output.write(rec_str[:-1] + "\n")
            
for (huisnr_id, huisnr_fields) in huisnr_dic.items():
    rec_str = ''
    for field in fields:
        value = ''
        if(field in huisnr_fields):
            rec_str += str(huisnr_fields[field]) + ','
        else:
            rec_str += ','
    output.write(rec_str[:-1] + "\n")

    if(len(args.output_osm) > 0):
        lat = str(huisnr_fields['LAT'])
        lon = str(huisnr_fields['LON'])
        if(len(lat) > 0 and len(lon) > 0):
            osm_id -= 1
            w.start("node", {"id": str(osm_id), "timestamp": dt.isoformat(), "version": "1", "visible": "true", "lon": lon, "lat": lat})
            w.element("tag", "", {"k": "addr:postal_code", "v": huisnr_fields['PKANCODE'})
            w.element("tag", "", {"k": "addr:street", "v": huisnr_fields['STREET_NL'})
            w.element("tag", "", {"k": "addr:house_number", "v": huisnr_fields['HUISNR'})
            w.element("tag", "", {"k": "addr:city", "v": huisnr_fields['COMMUNE_NL'})
            w.end()
if(len(args.output_osm) > 0):
    w.end()
output.close()

if (args.write_postcodes):
    for postalcode in pkancode_set:
        output = open(str(postalcode) + '.csv', 'w')
        rec_str = ''
        for field in fields:
            rec_str += field + ','
        output.write(rec_str[:-1] + "\n")

        for (huisnr_id, huisnr_fields) in huisnr_dic.items():
            rec_str = ''
            if(huisnr_fields['PKANCODE'] == postalcode):
                for field in fields:
                    value = ''
                    if(field in huisnr_fields):
                        rec_str += str(huisnr_fields[field]) + ','
                    else:
                        rec_str += ','
                output.write(rec_str[:-1] + "\n")
        output.close()
