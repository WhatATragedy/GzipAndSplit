
from handlers.routeviews import RouteViews
from handlers.gzipAndSplit import GzipAndSplit

if __name__ == '__main__':
    # routeviews = RouteViews()
    # routeviews.get_ribs('ribs', collector_list=['route-views.amsix'])
    zas = GzipAndSplit()
    # zas.gzipAndSplit("ribs", "zipped")
    zas.join("zipped", "zipped_back_again.tar.gz")