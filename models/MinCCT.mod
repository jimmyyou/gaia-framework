set N;
/* substrate nodes */

set F;
/* flows */

param b{u in N, v in N};
/* bandwidth of edge (u,v) */ 

param fs{i in F};
/* flow start points */

param fe{i in F};
/* flow end points */

param fv{i in F};
/* flow volumes */

var f{i in F, u in N, v in N} >= 0;
/* flow variable */

var alpha;
/* inverse CCT */

maximize A: alpha; 
/* minimizes CCT */

s.t. capcon{u in N, v in N}: sum{i in F} f[i,u,v] <= b[u,v];
/* capacity constraint */

s.t. demsat1{i in F}: sum{w in N} f[i, fs[i], w] - sum{w in N} f[i, w, fs[i]] = fv[i] * alpha;
s.t. demsat2{i in F}: sum{w in N} f[i, fe[i], w] - sum{w in N} f[i, w, fe[i]] = -fv[i] * alpha;
/* demand satisfaction */

s.t. flocon{i in F, u in N diff {fs[i], fe[i]}}: sum{w in N} f[i, u, w] - sum{w in N} f[i, w, u] = 0;
/* flow conservation */

end;
