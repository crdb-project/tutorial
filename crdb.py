import requests
import numpy as np


def query(num=None, energy_type="EK", **kwargs):
    """
    Queries the CRDB and returns the table as a numpy array.
    
    Parameters are passed as keywords directly to this function.
    
    Parameters
    ----------
    num: str
        Element, isotope, or particle.
    energy_type: str (default: EK)
        Energy unit for the requested quantity. Valid values: EKN, EK, R, ETOT.

    Other keyword-value pairs are interpreted as parameters for the query. See
    http://lpsc.in2p3.fr/crdb for a documentation which parameters are accepted.

    Passing an unknown parameter or using an unknown value results in an a ValueError.

    Returns
    -------
    numpy record array with the database content
    """
    if num is None:
        raise KeyError("setting parameter `num` is required")
    valid_energy_types = ("EKN", "EK", "R", "ETOT")
    if energy_type.upper() not in valid_energy_types:
        raise ValueError("energy_type must be one of " + ",".join(valid_energy_types))
    kwargs["num"] = num
    kwargs["energy_type"] = energy_type.upper()
    urlcmd = "http://lpsc.in2p3.fr/crdb/rest.php?" + "&".join(
        ["{0}={1}".format(k, v) for (k, v) in kwargs.items()]
    )
    data = requests.get(urlcmd).content.decode("utf-8").split("\n")
    if len(data) == 1:
        raise ValueError(data[0])
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
    return np.genfromtxt(data, fields)
