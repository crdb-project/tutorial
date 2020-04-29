import urllib.request as rq
import numpy as np
import warnings


def query(num, den=None, energy_type="R", url=None, **kwargs):
    """
    Queries the CRDB and returns the table as a numpy array.
    
    Parameters are passed as keywords directly to this function. All values are
    case insensitive.
    
    Parameters
    ----------
    num: str
        Element, isotope, or particle.
    energy_type: str (optional, default: "R")
        Energy unit for the requested quantity. Valid values: EKN, EK, R, ETOT.
    url: str (optional, default: None)
        URL to send the request to, defaults to the standard url. This is an expert
        option, users do not need to change this.
    kwargs: 
        Other keyword-value pairs are interpreted as parameters for the query.
        See http://lpsc.in2p3.fr/crdb for a documentation which parameters are accepted.

    Passing an unknown parameter or using an unknown value triggers a ValueError.

    Returns
    -------
    numpy record array with the database content
    """
    # "+" must be escaped in URL, see
    # https://en.wikipedia.org/wiki/Percent-encoding
    if num.lower() == "e+":
        num = "e%2B"

    # workaround for empty error message from CRDB
    valid_energy_types = ("EKN", "EK", "R", "ETOT")
    if energy_type.upper() not in valid_energy_types:
        raise ValueError("energy_type must be one of " + ",".join(valid_energy_types))

    # do the query
    kwargs["num"] = num
    kwargs["energy_type"] = energy_type.upper()
    urlcmd = (
        (url or "http://lpsc.in2p3.fr/crdb")
        + "/rest.php?"
        + "&".join(["{0}={1}".format(k, v) for (k, v) in kwargs.items()])
    )
    with rq.urlopen(urlcmd, timeout=5) as u:
        data = u.read().decode("utf-8").split("\n")

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

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return np.genfromtxt(data, fields)


def test_query():
    import pytest

    tab = query("H")
    assert len(tab) > 1

    tab = query("e+")
    assert len(tab) > 1

    tab = query("e-")
    assert len(tab) > 1

    tab = query("B", "C")
    assert len(tab) > 1

    with pytest.raises(ValueError):
        query("Foobar")

    with pytest.raises(ValueError):
        query("H", energy_type="Foobar")


if __name__ == "__main__":
    import pytest

    pytest.main([__file__])
