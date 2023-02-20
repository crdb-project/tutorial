import urllib.request as rq
import numpy as np
import warnings
import cachier
import datetime


@cachier.cachier(stale_after=datetime.timedelta(days=30))
def query(
    num: str,
    den: str = "",
    energy_type: str = "R",
    combo_level: int = 1,
    energy_convert_level: int = 1,
    flux_rescaling: float = 0.0,
    exp_dates: str = "",
    energy_start: float = 0.0,
    energy_stop: float = 0.0,
    time_start: str = "",
    time_stop: str = "",
    time_series: str = "",
    format: str = "",
    modulation: str = "",
    timeout: int = 120,
    server_url="http://lpsc.in2p3.fr/crdb",
):
    """
    Query CRDB and return table as a numpy array.

    See http://lpsc.in2p3.fr/crdb for documentation which parameters are accepted.
    All string values are case insensitive.

    Parameters
    ----------
    num: str
        Element, isotope, particle, or mass group.
    den: str, optional
        Element, isotope, particle, or mass group.
    energy_type: str, optional
        Energy unit for the requested quantity. Default is R.
        Valid values: EKN, EK, R, ETOT, ETOTN.
    combo_level: int, optional
        One of 0, 1, 2. Default is 1. Add combinations (ratio, products) of native data
        (from the same sub-exp at the same energy) that match quantities in list (e.g.
        compute B/C from native B and C). Three levels of combos are enabled: 0 (native
        data only, no combo), 1 (exact combos), or 2 (exact and approximate combos): in
        level 1, the mean energy (or energy bin) of the two quantities must be within 5%,
        whereas for level 2, it must be within 20%
    energy_convert_level: int, optional
        One of 0, 1, 2. Default is 1. Add data obtained from an exact or approximate
        energy_type conversion (from native to queried). Three levels of conversion are
        enabled: 0 (native data only, no conversion), 1 (exact conversion only, which
        applies to isotopic and leptonic fluxes), and 2 (exact and approximate
        conversions, the later applying to flux of elements and of groups of elements).
    flux_scaling: float, optional
        Flux is multiplied by E^flux_scaling.
    exp_dates: str, optional
        Comma-separated list (optional time intervals) of sub-experiment names.
    energy_start: float, optional
        Lower limit for energy_type.
    energy_stop: float, optional
        Upper limit for energy_type.
    time_start: str, optional
        Lower limit for interval selection 	YYYY[/MM] (2014, 2010/06).
    time_stop: str, optional
        Upper limit for interval selection 	YYYY[/MM] (2020, 2019/06).
    time_series: str, optional
        Whether to discard, select only, or keep time series data in query CRDB keywords
        ('no', 'only', 'all'). Default is 'no'.
    format: str, optional
        Output format, one of 'csv', 'usine', 'galprop'. Default is 'csv'.
    modulation: str, optional
        Source of Solar modulation values, one of 'USO05', 'USO17', 'GHE17'. Default is
        'GHE17'.
    timeout: int, optional
        Timeout for server response in seconds. Default is 60.
    server_url: str, optional
        URL to send the request to, default is http://lpsc.in2p3.fr/crdb). This is an
        expert option, users do not need to change this.

    Returns
    -------
    numpy record array with the database content

    Raises
    ------
    ValueError
        An invalid parameter value triggers a ValueError.
    TimeoutError
        If the server does not respond within the timeout time.

    Notes
    -----

    This function caches identical queries for 30 days. If you need to reset the cache,
    do::

        from crdb import query

        query.clear_cache()
    """

    url = _url(
        num=num,
        den=den,
        energy_type=energy_type,
        combo_level=combo_level,
        energy_convert_level=energy_convert_level,
        flux_rescaling=flux_rescaling,
        exp_dates=exp_dates,
        energy_start=energy_start,
        energy_stop=energy_stop,
        time_start=time_start,
        time_stop=time_stop,
        time_series=time_series,
        format=format,
        modulation=modulation,
        server_url=server_url,
    )
    return _load(url, timeout=timeout)


def _url(
    num: str,
    den: str = "",
    energy_type: str = "R",
    combo_level: int = 1,
    energy_convert_level: int = 1,
    flux_rescaling: float = 0.0,
    exp_dates: str = "",
    energy_start: float = 0.0,
    energy_stop: float = 0.0,
    time_start: str = "",
    time_stop: str = "",
    time_series: str = "",
    format: str = "",
    modulation: str = "",
    server_url="http://lpsc.in2p3.fr/crdb",
):
    """
    Build a query URL for the CRDB server.
    """

    # "+" must be escaped in URL, see
    # https://en.wikipedia.org/wiki/Percent-encoding
    if num.lower() == "e+":
        num = r"e%2B"

    # workaround for empty error message from CRDB
    valid_energy_types = ("EKN", "EK", "R", "ETOT", "ETOTN")
    if energy_type.upper() not in valid_energy_types:
        raise ValueError("energy_type must be one of " + ",".join(valid_energy_types))

    if combo_level not in (0, 1, 2):
        raise ValueError(f"invalid combo_level {combo_level}")

    if energy_convert_level not in (0, 1, 2):
        raise ValueError(f"invalid energy_convert_level {energy_convert_level}")

    if flux_rescaling < 0 or flux_rescaling > 2.5:
        raise ValueError(f"invalid flux_rescaling {flux_rescaling}")

    # do the query
    kwargs = {
        "num": num,
        "energy_type": energy_type.upper(),
    }
    if den:
        kwargs["den"] = den
    if combo_level != 1:
        kwargs["combo_level"] = combo_level
    if energy_convert_level != 1:
        kwargs["energy_convert_level"] = energy_convert_level
    if flux_rescaling:
        kwargs["flux_rescaling"] = flux_rescaling
    if exp_dates:
        kwargs["exp_dates"] = exp_dates
    if energy_start:
        kwargs["energy_start"] = energy_start
    if energy_stop:
        kwargs["energy_stop"] = energy_stop
    if time_start:
        kwargs["time_start"] = time_start
    if time_stop:
        kwargs["time_stop"] = time_stop
    if time_series:
        kwargs["time_series"] = time_series
    if format:
        kwargs["format"] = format
    if modulation:
        kwargs["modulation"] = modulation

    return f"{server_url}/rest.php?" + "&".join(
        ["{0}={1}".format(k, v) for (k, v) in kwargs.items()]
    )


def _load(url, timeout):
    # if there is a timeout error, we hide original long traceback from the internal
    # libs and instead show a simple traceback
    try:
        with rq.urlopen(url, timeout=timeout) as u:
            data = u.read().decode("utf-8").split("\n")
        timeout_error = False
    except TimeoutError:
        timeout_error = True

    if timeout_error:
        raise TimeoutError(
            f"Server did not respond within timeout={timeout} to url={url}"
        )

    # check for errors and display them
    if len(data) == 1:
        raise ValueError(data[0])

    # convert text to numpy record array
    fields = [
        ("quantity", "U10"),
        ("sub_exp", "U100"),
        ("e_axis", "U4"),
        ("e_mean", "f8"),
        ("e_low", "f8"),
        ("e_high", "f8"),
        ("value", "f8"),
        ("err_stat_minus", "f8"),
        ("err_stat_plus", "f8"),
        ("err_sys_minus", "f8"),
        ("err_sys_plus", "f8"),
        ("ads_url", "U32"),
        ("phi_in_mv", "f8"),
        ("distance_in_au", "f8"),
        ("datetime", "U100"),
        ("is_upper_limit", "?"),
    ]

    # workaround: if first line starts with <html>,
    # remove first and last line with html tags
    if data[0].startswith("<html>"):
        data = data[1:-1]

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return np.genfromtxt(data, fields)
