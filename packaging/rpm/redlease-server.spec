Name:           redlease-server
Version:        %{redlease_version}
Release:        1
Summary:        RedLease lock server launcher
License:        MIT
URL:            https://github.com/udovenkoav1981/RedLease
Source0:        redlease-server
Source1:        LICENSE
Source2:        redlease-server.service
Source3:        redlease-server.sysconfig

BuildArch:      x86_64
AutoReqProv:    no
Requires(post): shadow-utils
Requires(post): systemd
Requires(preun): systemd
Requires(postun): shadow-utils
Requires(postun): systemd

%description
Standalone plaintext RedLease lock server launcher. The package contains only
the statically linked executable; service lifecycle and configuration remain
the responsibility of the host application or system administrator.

%prep

%build

%install
install -Dpm 0755 %{SOURCE0} %{buildroot}%{_bindir}/redlease-server
install -Dpm 0644 %{SOURCE1} %{buildroot}/usr/share/licenses/%{name}/LICENSE
install -Dpm 0644 %{SOURCE2} %{buildroot}/usr/lib/systemd/system/redlease-server.service
install -Dpm 0644 %{SOURCE3} %{buildroot}%{_sysconfdir}/sysconfig/redlease-server

%post
if ! getent group redlease >/dev/null; then
    /usr/sbin/groupadd --system redlease || exit 1
fi
if ! getent passwd redlease >/dev/null; then
    /usr/sbin/useradd \
        --system \
        --gid redlease \
        --home-dir / \
        --shell /sbin/nologin \
        --no-create-home \
        --comment "RedLease server" \
        redlease || exit 1
fi
if command -v systemctl >/dev/null && [ -d /run/systemd/system ]; then
    systemctl daemon-reload || :
    if [ "$1" -gt 1 ]; then
        systemctl try-restart redlease-server.service || :
    fi
fi

%preun
if [ "$1" -eq 0 ] && command -v systemctl >/dev/null && [ -d /run/systemd/system ]; then
    systemctl disable --now redlease-server.service || :
fi

%postun
if command -v systemctl >/dev/null && [ -d /run/systemd/system ]; then
    systemctl daemon-reload || :
fi
if [ "$1" -eq 0 ]; then
    if getent passwd redlease >/dev/null; then
        /usr/sbin/userdel redlease || :
    fi
    if getent group redlease >/dev/null; then
        /usr/sbin/groupdel redlease || :
    fi
fi
:

%files
%license /usr/share/licenses/%{name}/LICENSE
%config(noreplace) %{_sysconfdir}/sysconfig/redlease-server
%{_bindir}/redlease-server
/usr/lib/systemd/system/redlease-server.service
