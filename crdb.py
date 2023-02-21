import urllib.request as rq
import numpy as np
import warnings
import cachier
import datetime

# from "Submit data" tab on CRDB website
VALID_NAMES = (
    "H",
    "He",
    "Li",
    "Be",
    "B",
    "C",
    "N",
    "O",
    "F",
    "Ne",
    "Na",
    "Mg",
    "Al",
    "Si",
    "P",
    "S",
    "Cl",
    "Ar",
    "K",
    "Ca",
    "Sc",
    "Ti",
    "V",
    "Cr",
    "Mn",
    "Fe",
    "Co",
    "Ni",
    "Cu",
    "Zn",
    "Ga",
    "Ge",
    "As",
    "Se",
    "Br",
    "Kr",
    "Rb",
    "Sr",
    "Y",
    "Zr",
    "Nb",
    "Mo",
    "Tc",
    "Ru",
    "Rh",
    "Pd",
    "Ag",
    "Cd",
    "In",
    "Sn",
    "Sb",
    "Te",
    "I",
    "Xe",
    "Cs",
    "Ba",
    "La",
    "Ce",
    "Pr",
    "Nd",
    "Pm",
    "Sm",
    "Eu",
    "Gd",
    "Tb",
    "Dy",
    "Ho",
    "Er",
    "Tm",
    "Yb",
    "Lu",
    "Hf",
    "Ta",
    "W",
    "Re",
    "Os",
    "Ir",
    "Pt",
    "Au",
    "Hg",
    "Tl",
    "Pb",
    "Bi",
    "Po",
    "At",
    "Rn",
    "Fr",
    "Ra",
    "Ac",
    "Th",
    "Pa",
    "U",
    "Np",
    "Pu",
    "Am",
    "Cm",
    "Bk",
    "Cf",
    "Es",
    "Zgeq1",
    "Zgeq2",
    "Zgeq3",
    "Zgeq4",
    "Zgeq5",
    "Zgeq6",
    "Zgeq7",
    "Zgeq8",
    "H-bar",
    "He-bar",
    "Li-bar",
    "Be-bar",
    "B-bar",
    "C-bar",
    "N-bar",
    "O-bar",
    "Zgeq1-bar",
    "Zgeq2-bar",
    "Zgeq3-bar",
    "Zgeq4-bar",
    "Zgeq5-bar",
    "Zgeq6-bar",
    "Zgeq7-bar",
    "Zgeq8-bar",
    "1H-bar",
    "2H-bar",
    "3He-bar",
    "4He-bar",
    "6Li-bar",
    "9Be-bar",
    "11B-bar",
    "12C-bar",
    "14N-bar",
    "16O-bar",
    "e-",
    "e+",
    "NU_E",
    "NU_M",
    "NU_T",
    "GAMMA",
    "e-+e+",
    "SubFe",
    "1H",
    "2H",
    "3He",
    "4He",
    "6Li",
    "7Li",
    "7Be",
    "9Be",
    "10B",
    "10Be",
    "11B",
    "12C",
    "13C",
    "14N",
    "14C",
    "15N",
    "16O",
    "17O",
    "18O",
    "19F",
    "20Ne",
    "21Ne",
    "22Ne",
    "23Na",
    "24Mg",
    "25Mg",
    "26Mg",
    "26Al",
    "27Al",
    "28Si",
    "29Si",
    "30Si",
    "31P",
    "32S",
    "33S",
    "34S",
    "35Cl",
    "36S",
    "36Ar",
    "36Cl",
    "37Cl",
    "37Ar",
    "38Ar",
    "39K",
    "40Ar",
    "40Ca",
    "40K",
    "41K",
    "41Ca",
    "42Ca",
    "43Ca",
    "44Ca",
    "44Ti",
    "45Sc",
    "46Ti",
    "46Ca",
    "47Ti",
    "48Ti",
    "48Ca",
    "48Cr",
    "49Ti",
    "49V",
    "50Ti",
    "50Cr",
    "50V",
    "51V",
    "51Cr",
    "52Cr",
    "53Cr",
    "53Mn",
    "54Cr",
    "54Fe",
    "54Mn",
    "55Mn",
    "55Fe",
    "56Fe",
    "56Ni",
    "57Fe",
    "57Co",
    "58Fe",
    "58Ni",
    "59Co",
    "59Ni",
    "60Ni",
    "60Fe",
    "61Ni",
    "62Ni",
    "63Cu",
    "64Ni",
    "64Zn",
    "65Cu",
    "66Zn",
    "67Zn",
    "68Zn",
    "70Zn",
    "H-He-group",
    "N-group",
    "O-group",
    "Al-group",
    "Si-group",
    "Fe-group",
    "O-Fe-group",
    "C-Fe-group",
    "AllParticles",
    "<LnA>",
    "<X_max>",
    "X_mu_max",
    "<rho_mu_600>",
    "<rho_mu_800>",
    "<R_mu>",
    "LS-group",
    "HS-group",
    "Pt-group",
    "Pb-group",
    "Subactinides",
    "Actinides",
    "Z_33-34",
    "Z_35-36",
    "Z_37-38",
    "Z_39-40",
    "Z_41-42",
    "Z_43-44",
    "Z_45-46",
    "Z_47-48",
    "Z_49-50",
    "Z_51-52",
    "Z_53-54",
    "Z_55-56",
    "Z_57-58",
    "Z_59-60",
    "Zgeq70",
    "9Be+10Be",
)


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
    num = num.replace("+", "%2B")
    if den:
        den = den.replace("+", "%2B")

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


@cachier.cachier(stale_after=datetime.timedelta(days=30))
def _server_request(url, timeout):
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

    return data


def _load(url, timeout):
    data = _server_request(url, timeout)

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
        table = np.genfromtxt(data, fields)

    # workaround: replace &amp; in sub_exp strings
    sub_exps = np.unique(table["sub_exp"])
    code = "&amp;"
    for sub_exp in sub_exps:
        if code not in sub_exp:
            continue
        mask = table["sub_exp"] == sub_exp
        table["sub_exp"][mask] = sub_exp.replace(code, "&")
    return table


def experiment_masks(table):
    """
    Generate masks which select all points from each experiment.

    This returns a dict which maps the experiment name to the mask. Different data taking
    campains are joined.
    """
    # generate a mask per experiment, see `CRDB REST query tutorial.ipynb` for details
    experiments = {}
    for this_sub_exp in np.unique(table["sub_exp"]):
        exp = this_sub_exp[: this_sub_exp.find("(")]
        mask = table["sub_exp"] == this_sub_exp
        exp_mask = experiments.get(exp, False)
        exp_mask |= mask
        experiments[exp] = exp_mask
    return experiments


def clear_cache():
    """
    Delete the local CRDB cache.
    """
    _server_request.clear_cache()
