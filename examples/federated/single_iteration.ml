const bins: {bins};
vectors = scan({relation});

-------------------------
-- Constants + Functions
-------------------------
const alpha: 1.0;

def iif(expression, true_value, false_value):
    case when expression then true_value
         else false_value end;
def bin(x, high, low): greater(least(int((bins-1) * (x - low) / iif(high != low, high - low, 1)),
                                bins - 1), 0);
def difference(current, previous, previous_time, time):
    iif(previous_time >= 0,
        (current - previous) * iif(previous_time < time, 1, -1),
        current);

------------------------------------------------------------------------------------
-- Harr Transform
------------------------------------------------------------------------------------
uda HarrTransformGroupBy(alpha, time, x) {{
  [0.0 as coefficient, 0.0 as _sum, 0 as _count, -1 as _time];
  [difference(x, coefficient, _time, time), _sum + x, _count + 1, time];
  [coefficient, _sum / int(_count * alpha)];
}};

groups = [from vectors emit
                 id,
                 int(floor(time/2)) as time,
                 HarrTransformGroupBy(alpha, time, value) as [coefficient, mean]];

coefficients = [from groups emit id, coefficient];
range = [from vectors emit max(value) - min(value) as high, min(value) - max(value) as low];

histogram = [from coefficients, range
             emit id,
                  bin(coefficient, high, low) as index,
                  count(bin(coefficient, high, low)) as value];

sink(histogram);