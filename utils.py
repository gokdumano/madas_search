from xml.etree import ElementTree as ET
from multiprocessing.pool import Pool
from multiprocessing import cpu_count
from urllib.parse import urlencode
from datetime import datetime
from requests import Session

import itertools as it, csv, os

def downloadRaster(entry, odir):
    id_         = entry['id']
    raster_link = entry['raster_link']
    
    with Session() as s:
        raster_response = s.get(raster_link, stream=True)
        raster_response.raise_for_status()
        
        raster_path = os.path.join(odir, f'{id_}.tar.bz2')
        
        with open(raster_path, 'wb') as raster_file:
            for chunk in raster_response.iter_content(chunk_size=1024):
                if chunk: raster_file.write(chunk)
            print(f'Downloaded ... {id_}')

def write2CSV(entries, csvpath):
    fieldnames = ['id', 'raster_link', 'published', 'polygon',
                  'tirPointingAngle', 'sar_swirPointingAngle', 'sar_vnirPointingAngle',
                  'eop_illuminationElevationAngle', 'eop_illuminationAzimuthAngle',
                  'opt_cloudCoverPercentage', 'eop_sensorOperationalMode',
                  'sar_acrossTrackIncidenceAngle']
    
    if not csvpath.endswith('.csv'): csvpath += '.csv'
    with open(csvpath, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for entry in entries:
            writer.writerow(entry)

def entry_parser(entry):
    id_ = entry.find('{http://www.w3.org/2005/Atom}id').text
    return {
        'id': id_,
        'raster_link': f'https://aster.geogrid.org/ASTER/fetchL3A/{id_}.tar.bz2',
        'published': entry.find('{http://www.w3.org/2005/Atom}published').text,
        'polygon': entry.find('{http://www.w3.org/2005/Atom}georss_polygon').text,
        'tirPointingAngle': entry.find('*//{http://www.w3.org/2005/Atom}sar_tirPointingAngle').text,
        'sar_swirPointingAngle': entry.find('*//{http://www.w3.org/2005/Atom}sar_swirPointingAngle').text,
        'sar_vnirPointingAngle': entry.find('*//{http://www.w3.org/2005/Atom}sar_vnirPointingAngle').text,
        'eop_illuminationElevationAngle': entry.find('*//{http://www.w3.org/2005/Atom}eop_illuminationElevationAngle').text,
        'eop_illuminationAzimuthAngle': entry.find('*//{http://www.w3.org/2005/Atom}eop_illuminationAzimuthAngle').text,
        'opt_cloudCoverPercentage': entry.find('*//{http://www.w3.org/2005/Atom}opt_cloudCoverPercentage').text,
        'eop_sensorOperationalMode': entry.find('*//{http://www.w3.org/2005/Atom}eop_sensorOperationalMode').text,
        'sar_acrossTrackIncidenceAngle': entry.find('*//{http://www.w3.org/2005/Atom}sar_acrossTrackIncidenceAngle').text
        }

def date_parser(arg):
    arg = arg.upper().replace('.', '-')
    if arg == 'START': return '1999-12-18+00:00:00'
    elif arg == 'CURRENT': return datetime.now().strftime('%Y-%m-%d+00:00:00')
    else: return datetime.strptime(arg, '%Y-%m-%d').strftime('%Y-%m-%d+00:00:00')

def Search(obs_period, bbox, day_or_night, op_mode, cloud, ie_angle, p_angle):
    max_record = 100
    start, end = obs_period
    minx, miny, maxx, maxy = bbox
    ASTER5, ASTER6 = {'Day':('TRUE', 'FALSE'), 'Night':('FALSE', 'TRUE')}.get(day_or_night, ('TRUE', 'TRUE'))
    
    params = {
        'max_record': max_record,
        'sort': 'DESC',
        'start': start,
        'end': end,
        'minx': minx,
        'miny': miny,
        'maxx': maxx,
        'maxy': maxy,
        'aster_op_mode': op_mode,
        'ASTER5': ASTER5,
        'ASTER6': ASTER6,
        'aster_cloud': cloud
        }

    if ie_angle is not None:
        ASTER7, ASTER8 = ie_angle
        params.update({'ASTER7': ASTER7, 'ASTER8': ASTER8})

    if p_angle is not None:
        ASTER10, ASTER11 = p_angle
        params.update({'ASTER10': ASTER10, 'ASTER11': ASTER11})

    url = 'https://gbank.gsj.jp/madas/cgi-bin/php/SearchCSW.php'
    entries = []

    with Session() as s:
        for page in it.count(1):
            params.update({'page': page})
            response = s.get(url, params=urlencode(params, safe = '+:'))
            response.raise_for_status()
            
            html = ET.fromstring(response.content)

            entry = html.findall('{http://www.w3.org/2005/Atom}entry')
            entries.extend(entry)
            print(f'\r[Page.{page}] - Found {len(entries)}', end='')
            if len(entry) < max_record: break
            
    print('\nUnpacking entries...', end='')
    entries = [entry_parser(entry) for entry in entries]
    print('done')
    return entries

def Download(csvpath, odir):
    n_cores = cpu_count()
    if not os.path.isdir(odir): os.makedirs(odir)
    with open(csvpath, newline='') as csvfile, Pool(n_cores) as pool:
        entries = csv.DictReader(csvfile)
        items   = ((entry, odir) for entry in entries)
        pool.starmap(downloadRaster, items)