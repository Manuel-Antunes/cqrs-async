# Draws the logo: Ferris-style crab juggling events from the write side
# (left claw, amber) to the read side (right claw, blue).
#   python3 scripts/logo.py static/img
# writes logo.svg, logo-animated.svg (events in flight) and favicon.svg.
import math, sys
OUT = sys.argv[1]

ORANGE, SHADE, DARK = '#F74C00', '#D63F00', '#A52B00'
WRITE, MID, READ = '#FBBF24', '#C084FC', '#38BDF8'

def f(v): return ('%.1f' % v).rstrip('0').rstrip('.')

# --- shell: half ellipse with soft spikes on top, rounded belly -------------
cx, cy, rx, ry, h = 120, 122, 72, 44, 9
def ell(t): return (cx - rx*math.cos(t), cy - ry*math.sin(t))
def normal(t):
    dx, dy = rx*math.sin(t), -ry*math.cos(t)          # d/dt of ell
    n = (dy, -dx)                                      # outward (up)
    l = math.hypot(*n); return (n[0]/l, n[1]/l)
n_spikes, t0, t1 = 8, 0.13*math.pi, 0.87*math.pi
valleys = [t0 + (t1-t0)*k/n_spikes for k in range(n_spikes+1)]
d = 'M%s %s ' % tuple(map(f, ell(0)))
d += 'A%d %d 0 0 1 %s %s ' % (rx, ry, *map(f, ell(valleys[0])))
for a, b in zip(valleys, valleys[1:]):
    m = (a+b)/2; p = ell(m); n = normal(m)
    c = (p[0]+n[0]*2*h, p[1]+n[1]*2*h)
    d += 'Q%s %s %s %s ' % (*map(f, c), *map(f, ell(b)))
d += 'A%d %d 0 0 1 %s %s ' % (rx, ry, *map(f, ell(math.pi)))
d += 'C192 147 162 156 120 156 C78 156 48 147 48 122Z'
SHELL = d

# --- the throw: a cubic arc from the left claw to the right one ------------
P = [(47, 44), (76, -8), (164, -8), (193, 44)]
def bez(t):
    u = 1-t; c = (u**3, 3*u*u*t, 3*u*t*t, t**3)
    return tuple(sum(ci*p[i] for ci, p in zip(c, P)) for i in (0, 1))
def tangent(t):
    u = 1-t
    return tuple(3*u*u*(P[1][i]-P[0][i]) + 6*u*t*(P[2][i]-P[1][i]) + 3*t*t*(P[3][i]-P[2][i]) for i in (0, 1))
ARC = 'M%d %d C%d %d %d %d %d %d' % (*P[0], *P[1], *P[2], *P[3])
at = bez(0.9); tg = tangent(0.9); ang = math.degrees(math.atan2(tg[1], tg[0]))

def claw(cx, cy, mouth_deg, mirror):
    half = 36
    a1, a2 = math.radians(mouth_deg-half), math.radians(mouth_deg+half)
    p1 = (cx+21*math.cos(a1), cy+21*math.sin(a1)); p2 = (cx+21*math.cos(a2), cy+21*math.sin(a2))
    apex = (cx-2*math.cos(math.radians(mouth_deg)), cy-2*math.sin(math.radians(mouth_deg)))
    return 'M%s %s L%s %s A21 21 0 1 0 %s %s Z' % (*map(f, apex), *map(f, p1), *map(f, p2)) if not mirror else \
           'M%s %s L%s %s A21 21 0 1 1 %s %s Z' % (*map(f, apex), *map(f, p2), *map(f, p1))

DEFS = f'''<defs>
  <clipPath id="shell"><path d="{SHELL}"/></clipPath>
  <linearGradient id="flight" x1="54" y1="0" x2="186" y2="0" gradientUnits="userSpaceOnUse">
    <stop offset="0" stop-color="{WRITE}"/><stop offset=".5" stop-color="{MID}"/><stop offset="1" stop-color="{READ}"/>
  </linearGradient>
</defs>'''

CRAB = f'''<g stroke="{DARK}" stroke-width="7" stroke-linecap="round" stroke-linejoin="round" fill="none">
  <path d="M64 136 L45 146 L41 161 M74 144 L60 157 L58 171 M88 150 L80 163 L80 175"/>
  <path d="M176 136 L195 146 L199 161 M166 144 L180 157 L182 171 M152 150 L160 163 L160 175"/>
</g>
<g stroke="{ORANGE}" stroke-width="12" stroke-linecap="round" fill="none">
  <path d="M60 116 C42 114 32 100 34 84"/><path d="M180 116 C198 114 208 100 206 84"/>
</g>
<path d="{claw(36, 70, -70, False)}" fill="{ORANGE}"/>
<path d="{claw(204, 70, -110, True)}" fill="{ORANGE}"/>
<path d="{SHELL}" fill="{ORANGE}"/>
<g clip-path="url(#shell)">
  <ellipse cx="120" cy="166" rx="92" ry="34" fill="{SHADE}"/>
  <ellipse cx="94" cy="96" rx="22" ry="8" fill="#fff" opacity=".18" transform="rotate(-16 94 96)"/>
</g>
<ellipse cx="104" cy="110" rx="8" ry="10" fill="#1C0A02"/><ellipse cx="136" cy="110" rx="8" ry="10" fill="#1C0A02"/>
<circle cx="106.5" cy="105.5" r="3" fill="#fff"/><circle cx="138.5" cy="105.5" r="3" fill="#fff"/>
<path d="M110 128 Q120 139 130 128 Q120 132 110 128Z" fill="#1C0A02" stroke="#1C0A02" stroke-width="2" stroke-linejoin="round"/>'''

TRAIL = f'''<path d="{ARC}" stroke="url(#flight)" stroke-width="3" stroke-linecap="round" stroke-dasharray="0.1 8" fill="none"/>
<path d="M-5 -6.5 L7 0 L-5 6.5 Z" transform="translate({f(at[0])} {f(at[1])}) rotate({f(ang)})" fill="{READ}" stroke="{READ}" stroke-width="2" stroke-linejoin="round"/>'''

def still_events():
    out = ''
    for t, r, c in ((0.27, 6.5, WRITE), (0.5, 7.5, MID), (0.73, 6.5, READ)):
        x, y = bez(t); out += f'<circle cx="{f(x)}" cy="{f(y)}" r="{r}" fill="{c}"/>'
    return out

def moving_events():
    out = ''
    for i in range(4):
        b = '%.2fs' % (i*0.6)
        out += (f'<circle r="7" fill="{WRITE}">'
                f'<animateMotion dur="2.4s" begin="{b}" repeatCount="indefinite" path="{ARC}"/>'
                f'<animate attributeName="fill" values="{WRITE};{MID};{READ}" dur="2.4s" begin="{b}" repeatCount="indefinite"/>'
                f'<animate attributeName="r" values="4;7.5;4" dur="2.4s" begin="{b}" repeatCount="indefinite"/>'
                f'<animate attributeName="opacity" values="0;1;1;0" keyTimes="0;.12;.88;1" dur="2.4s" begin="{b}" repeatCount="indefinite"/>'
                '</circle>')
    return out

def svg(vb, body, title='cqrs_async'):
    return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{vb}" fill="none" role="img" aria-label="{title}">'
            f'<title>{title}</title>{DEFS}{body}</svg>\n')

TIGHT = '8 -6 224 186'
open(f'{OUT}/logo.svg', 'w').write(svg(TIGHT, TRAIL + still_events() + CRAB))
style = '<style>.moving{display:inline}.still{display:none}@media (prefers-reduced-motion: reduce){.moving{display:none}.still{display:inline}}</style>'
open(f'{OUT}/logo-animated.svg', 'w').write(svg(TIGHT, style + TRAIL + f'<g class="still">{still_events()}</g><g class="moving">{moving_events()}</g>' + CRAB))
# Square, a little padding: the favicon.
# Square, the crab alone: the arc is too thin to read at 16 px.
open(f'{OUT}/favicon.svg', 'w').write(svg('10 5 220 220', CRAB))
