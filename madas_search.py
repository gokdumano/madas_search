from utils import Download, Search, date_parser, write2CSV
from argparse import ArgumentParser

if __name__ == '__main__':
    op_modes    = {'FULL':'Full', 'VST':'VST', 'V':'V', 'T':'T', 'ST':'ST'}
    # ------------------------------------------------
    parser      = ArgumentParser(prog='PROG')
    subparsers  = parser.add_subparsers(help='sub-command help')
    parser_a    = subparsers.add_parser('search', help='Search ASTB tiles from MADAS and save results as CSV file')
    parser_b    = subparsers.add_parser('download', help='Iter results from CSV files and download entries')
    # ------------------------------------------------
    parser_a.set_defaults(which='search')
    parser_a.add_argument('--bbox'        , required=True , metavar=('minLng', 'minLat', 'maxLng', 'maxLat'), nargs=4, type=float)
    parser_a.add_argument('--csvpath'     , required=True , metavar='csvPath')
    parser_a.add_argument('--obs_period'  , required=False, metavar=('from', 'to'), nargs=2, default=('1999-12-18+00:00:00', date_parser('CURRENT')), type=lambda arg: date_parser(arg))
    parser_a.add_argument('--op_mode'     , required=False, choices=('Full', 'VST', 'V', 'T', 'ST'), default='V', type=lambda arg: op_modes.get(arg.upper()))
    parser_a.add_argument('--day_or_night', required=False, choices=('Both', 'Day', 'Night'), default='Day', type=lambda arg: arg.capitalize())
    parser_a.add_argument('--ie_angle'    , required=False, metavar=('from', 'to'), nargs=2, default=None, type=float)
    parser_a.add_argument('--cloud'       , required=False, metavar='cloud', default=100, type=int)
    parser_a.add_argument('--p_angle'     , required=False, metavar=('from', 'to'), nargs=2, default=None, type=float)
    # ------------------------------------------------
    parser_b.set_defaults(which='download')
    parser_b.add_argument('--csvpath', required=True , metavar='csvPath')
    parser_b.add_argument('--odir'   , required=True , metavar='odir'   )

    args = parser.parse_args()
    if args.which == 'search':
        obs_period   = args.obs_period
        bbox         = args.bbox
        day_or_night = args.day_or_night
        op_mode      = args.op_mode
        cloud        = args.cloud
        ie_angle     = args.ie_angle
        p_angle      = args.p_angle
        csvpath      = args.csvpath
        
        entries = Search(obs_period, bbox, day_or_night, op_mode, cloud, ie_angle, p_angle)
        write2CSV(entries, csvpath)
        
    elif args.which == 'download':
        csvpath = args.csvpath
        odir    = args.odir        
        Download(csvpath, odir)