/*****************************************************************************
 *
 * This file is part of Mapnik (c++ mapping toolkit)
 *
 * Copyright (C) 2006 Artem Pavlenko, Jean-Francois Doyon
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 *
 *****************************************************************************/
// boost
#include <boost/foreach.hpp>
#include <boost/make_shared.hpp>
#include <boost/python.hpp>
#include <boost/python/stl_iterator.hpp>

#include "python_datasource.hpp"

using boost::python::object;

namespace
{

// Use RAII to acquire and release the GIL as needed.
class ensure_gil
{
  public:
    ensure_gil() : gil_state_(PyGILState_Ensure()) {}
    ~ensure_gil() { PyGILState_Release(gil_state_); }

  protected:
    PyGILState_STATE gil_state_;
};

std::string extractException()
{
    using namespace boost::python;

    PyObject *exc, *val, *tb;
    PyErr_Fetch(&exc, &val, &tb);
    PyErr_NormalizeException(&exc, &val, &tb);
    handle<> hexc(exc), hval(allow_null(val)), htb(allow_null(tb));
    if (!hval)
    {
        return extract<std::string>(str(hexc));
    }
    else
    {
        object traceback(import("traceback"));
        object format_exception(traceback.attr("format_exception"));
        object formatted_list(format_exception(hexc, hval, htb));
        object formatted(str("").join(formatted_list));
        return extract<std::string>(formatted);
    }
}

mapnik::parameters ds_to_params(object const &ds)
{
    mapnik::parameters ps;
    ps.insert(mapnik::parameter("type", "python"));
    return ps;
}

} // end anonymous namespace

namespace mapnik
{

python_datasource::python_datasource(object const &ds)
    : datasource(ds_to_params(ds)),
      desc_(python_datasource::name(), "utf-8"),
      datasource_(ds)
{
}

python_datasource::~python_datasource() {}

const char *python_datasource::name()
{
    return "python";
}

layer_descriptor python_datasource::get_descriptor() const
{
    return desc_;
}

datasource::datasource_t python_datasource::type() const
{
    try
    {
        ensure_gil lock;
        object data_type = get_param("data_type").value();
        long data_type_integer = boost::python::extract<long>(data_type);
        return datasource::datasource_t(data_type_integer);
    }
    catch (boost::python::error_already_set)
    {
        throw datasource_exception(extractException());
    }
}

boost::optional<object>
python_datasource::get_param(std::string const &key) const
{
    object v = datasource_.attr(key.c_str());
    if (v.is_none())
    {
        return boost::none;
    }
    return v;
}

bool python_datasource::has_param(std::string const &key) const
{
    return !(get_param(key) == boost::none);
}

box2d<double> python_datasource::envelope() const
{
    box2d<double> box;
    try
    {
        ensure_gil lock;
        if (!has_param("envelope"))
        {
            throw datasource_exception("Python: could not access envelope property");
        }
        else
        {
            object py_envelope = get_param("envelope").value();
            if (py_envelope.is_none())
            {
                throw datasource_exception("Python: envelope property is None");
            }
            else
            {
                boost::python::extract<double> ex(py_envelope.attr("minx"));
                if (!ex.check())
                    throw datasource_exception("Python: could not convert envelope.minx");
                box.set_minx(ex());
                boost::python::extract<double> ex1(py_envelope.attr("miny"));
                if (!ex1.check())
                    throw datasource_exception("Python: could not convert envelope.miny");
                box.set_miny(ex1());
                boost::python::extract<double> ex2(py_envelope.attr("maxx"));
                if (!ex2.check())
                    throw datasource_exception("Python: could not convert envelope.maxx");
                box.set_maxx(ex2());
                boost::python::extract<double> ex3(py_envelope.attr("maxy"));
                if (!ex3.check())
                    throw datasource_exception("Python: could not convert envelope.maxy");
                box.set_maxy(ex3());
            }
        }
    }
    catch (boost::python::error_already_set)
    {
        throw datasource_exception(extractException());
    }
    return box;
}

boost::optional<datasource_geometry_t> python_datasource::get_geometry_type() const
{
    typedef boost::optional<datasource_geometry_t> return_type;

    try
    {
        ensure_gil lock;
        // if the datasource object has no geometry_type attribute, return a 'none' value
        if (!has_param("geometry_type"))
        {
            return return_type();
        }
        object py_geometry_type =
            get_param("geometry_type").value();
        // if the attribute value is 'None', return a 'none' value
        if (py_geometry_type.is_none())
        {
            return return_type();
        }
        long geom_type_integer = boost::python::extract<long>(py_geometry_type);
        return datasource_geometry_t(geom_type_integer);
    }
    catch (boost::python::error_already_set)
    {
        throw datasource_exception(extractException());
    }
}

featureset_ptr python_datasource::features(query const &q) const
{
    try
    {
        // if the query box intersects our world extent then query for features
        if (envelope().intersects(q.get_bbox()))
        {
            ensure_gil lock;
            object features(get_param("features").value()(q));
            // if 'None' was returned, return an empty feature set
            if (features.is_none())
            {
                return featureset_ptr();
            }
            return std::make_shared<python_featureset>(features);
        }
        // otherwise return an empty featureset pointer
        return featureset_ptr();
    }
    catch (boost::python::error_already_set)
    {
        throw datasource_exception(extractException());
    }
}

featureset_ptr python_datasource::features_at_point(coord2d const &pt, double tol) const
{

    try
    {
        ensure_gil lock;
        object features(get_param("features_at_point").value()(pt));
        // if we returned none, return an empty set
        if (features.is_none())
        {
            return featureset_ptr();
        }
        // otherwise, return a feature set which can iterate over the iterator
        return std::make_shared<python_featureset>(features);
    }
    catch (boost::python::error_already_set)
    {
        throw datasource_exception(extractException());
    }
}

python_featureset::python_featureset(object iterator)
{
    ensure_gil lock;
    begin_ = boost::python::stl_input_iterator<feature_ptr>(iterator);
}

python_featureset::~python_featureset()
{
    ensure_gil lock;
    begin_ = end_;
}

feature_ptr python_featureset::next()
{
    // checking to see if we've reached the end does not require the GIL.
    if (begin_ == end_)
        return feature_ptr();

    // getting the next feature might call into the interpreter and so the GIL must be held.
    ensure_gil lock;

    return *(begin_++);
}

} // end namespace mapnik
