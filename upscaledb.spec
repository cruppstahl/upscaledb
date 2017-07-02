
Summary: upscaledb embedded database engine
Name: upscaledb
Version: 2.2.1
Release: 1%{?dist}
Source0: https://upscaledb.com/public/dl/%{name}-%{version}.tar.gz
URL: https://upscaledb.com
License: APLv2
Group: System Environment/Libraries
BuildRequires: protobuf-devel, protobuf-compiler, libtool, boost-devel, snappy-devel, gperftools
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

%description
A lightweight embedded key-value store with a built-in query language.

%package devel
Summary: Upscaledb development files for C/C++
Group: Development/Libraries
Requires: %{name} = %{version}-%{release}

%description devel
A lightweight embedded key-value store with a built-in query language.

%package static
Summary: Upscaledb static libraries
Group: Development/Libraries
Requires: %{name} = %{version}-%{release}
Requires: %{name}-devel = %{version}-%{release}

%description static
A lightweight embedded key-value store with a built-in query language.

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
%{_includedir}/ups

%files static
%defattr(-,root,root,-)
%{_libdir}/libupscaledb.a
%{_libdir}/libupsserver.a




%changelog
* Sat Jan 29 2011 hamster@gunkel.ca - 1.1.8-1
- initial version of spec file
- rest of changelog goes here

