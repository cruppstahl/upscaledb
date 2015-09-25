
Summary: upscaledb embedded database engine
Name: upscaledb
Version: 2.1.11
Release: 1%{?dist}
Source0: http://upscaledb.com/public/dl/%{name}-%{version}.tar.gz
URL: http://upscaledb.com
License: GPL 3.0
Group: System Environment/Libraries
BuildRequires: protobuf-devel, libtool, curl-devel
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

%description
A lightweight embedded key-value store with built-in analytical functions..

%package devel
Summary: C development files for the upscaledb library
Group: Development/Libraries
Requires: %{name} = %{version}-%{release}

%description devel
A lightweight embedded key-value store with built-in analytical functions..

%package static
Summary: upscaledb static libraries
Group: Development/Libraries
Requires: %{name} = %{version}-%{release}
Requires: %{name}-devel = %{version}-%{release}

%description static
A lightweight embedded key-value store with built-in analytical functions..

%prep
%setup -q

%build
#autoconf
export CFLAGS="-ggdb2 -O3"
CFLAGS="$RPM_OPT_FLAGS -fno-strict-aliasing"; export CFLAGS
%configure
make %{?_smp_mflags}


%install
rm -rf %{buildroot}
make DESTDIR=%{buildroot} prefix=%{_prefix} libdir=%{_libdir}  install
rm -fv ${RPM_BUILD_ROOT}%{_libdir}/*.la


%clean
rm -rf %{buildroot}

%post -p /sbin/ldconfig

%postun -p /sbin/ldconfig

%post -p /sbin/ldconfig devel

%postun -p /sbin/ldconfig devel

%post -p /sbin/ldconfig static

%postun -p /sbin/ldconfig static


%files
%defattr(-,root,root,-)
%doc AUTHORS COPYING README
%{_bindir}/*
%{_libdir}/*.so.*

%files devel
%defattr(-,root,root,-)
%doc    documentation/*
%{_libdir}/*.so
%{_includedir}/ham

%files static
%defattr(-,root,root,-)
%{_libdir}/libupscaledb.a
%{_libdir}/libhamserver.a




%changelog
* Sat Jan 29 2011 hamster@gunkel.ca - 1.1.8-1
- initial version of spec file
- rest of changelog goes here

