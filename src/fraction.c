/**
 * Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 */

#include "config.h"

#include <string.h>
#include <math.h>
#include <float.h>
#include <ham/hamsterdb.h>
#include "fraction.h"
#include "error.h"



/* fraction code ripped from my patch for meGUI */
/**
* Code according to info found here: http://mathforum.org/library/drmath/view/51886.html
* 
* 
* Date: 06/29/98 at 13:12:44
* 
* From: Doctor Peterson
* 
* Subject: Re: Decimal To Fraction Conversion
* 
* 
* The algorithm I am about to show you has an interesting history. I 
* recently had a discussion with a teacher in England who had a 
* challenging problem he had given his students, and wanted to know what 
* others would do to solve it. The problem was to find the fraction 
* whose decimal value he gave them, which is essentially identical to 
* your problem! I wasn't familiar with a standard way to do it, but 
* solved it by a vaguely remembered Diophantine method. Then, my 
* curiosity piqued, and I searched the Web for information on the 
* problem and didn't find it mentioned in terms of finding the fraction 
* for an actual decimal, but as a way to approximate an irrational by a 
* fraction, where the continued fraction method was used. 
* 
* 
* I wrote to the teacher, and he responded with a method a student of 
* his had come up with, which uses what amounts to a binary search 
* technique. I recognized that this produced the same sequence of 
* approximations that continued fractions gave, and was able to 
* determine that it is really equivalent, and that it is known to some 
* mathematicians (or at least math historians). 
* 
* 
* After your request made me realize that this other method would be 
* easier to program, I thought of an addition to make it more efficient, 
* which to my knowledge is entirely new. So we're either on the cutting 
* edge of computer technology or reinventing the wheel, I'm not sure 
* which!
* 
* 
* Here's the method, with a partial explanation for how it works:
* 
* 
* We want to approximate a value m (given as a decimal) between 0 and 1, 
* by a fraction Y/X. Think of fractions as vectors (denominator, 
* numerator), so that the slope of the vector is the value of the 
* fraction. We are then looking for a lattice vector (X, Y) whose slope 
* is as close as possible to m. This picture illustrates the goal, and 
* shows that, given two vectors A and B on opposite sides of the desired 
* slope, their sum A + B = C is a new vector whose slope is between the 
* two, allowing us to narrow our search:
* 
* <pre>
* num
* ^
* |
* +  +  +  +  +  +  +  +  +  +  +
* |
* +  +  +  +  +  +  +  +  +  +  +
* |                                  slope m=0.7
* +  +  +  +  +  +  +  +  +  +  +   /
* |                               /
* +  +  +  +  +  +  +  +  +  +  D &lt;--- solution
* |                           /
* +  +  +  +  +  +  +  +  + /+  +
* |                       /
* +  +  +  +  +  +  +  C/ +  +  +
* |                   /
* +  +  +  +  +  + /+  +  +  +  +
* |              /
* +  +  +  +  B/ +  +  +  +  +  +
* |          /
* +  +  + /A  +  +  +  +  +  +  +
* |     /
* +  +/ +  +  +  +  +  +  +  +  +
* | /
* +--+--+--+--+--+--+--+--+--+--+--&gt; denom
* </pre>
* 
* 
* Here we start knowing the goal is between A = (3,2) and B = (4,3), and 
* formed a new vector C = A + B. We test the slope of C and find that 
* the desired slope m is between A and C, so we continue the search 
* between A and C. We add A and C to get a new vector D = A + 2*B, which 
* in this case is exactly right and gives us the answer.
* 
* 
* Given the vectors A and B, with slope(A) &lt; m &lt; slope(B), 
* we can find consecutive integers M and N such that 
* slope(A + M*B) &lt; x &lt; slope(A + N*B) in this way:
* 
* 
* If A = (b, a) and B = (d, c), with a/b &lt; m &lt; c/d, solve
* 
* <pre>
*     a + x*c
*     ------- = m
*     b + x*d
* </pre>
* 
* 
* to give
* 
* <pre>
*         b*m - a
*     x = -------
*         c - d*m
* </pre>
* 
* 
* If this is an integer (or close enough to an integer to consider it 
* so), then A + x*B is our answer. Otherwise, we round it down and up to 
* get integer multipliers M and N respectively, from which new lower and 
* upper bounds A' = A + M*B and B' = A + N*B can be obtained. Repeat the 
* process until the slopes of the two vectors are close enough for the 
* desired accuracy. The process can be started with vectors (0,1), with 
* slope 0, and (1,1), with slope 1. Surprisingly, this process produces 
* exactly what continued fractions produce, and therefore it will 
* terminate at the desired fraction (in lowest terms, as far as I can 
* tell) if there is one, or when it is correct within the accuracy of 
* the original data.
* 
* 
* For example, for the slope 0.7 shown in the picture above, we get 
* these approximations:
* 
* 
* Step 1: A = 0/1, B = 1/1 (a = 0, b = 1, c = 1, d = 1)
* 
* <pre>
*         1 * 0.7 - 0   0.7
*     x = ----------- = --- = 2.3333
*         1 - 1 * 0.7   0.3
* 
*     M = 2: lower bound A' = (0 + 2*1) / (1 + 2*1) = 2 / 3
*     N = 3: upper bound B' = (0 + 3*1) / (1 + 3*1) = 3 / 4
* </pre>
* 
* 
* Step 2: A = 2/3, B = 3/4 (a = 2, b = 3, c = 3, d = 4)
* 
* <pre>
*         3 * 0.7 - 2   0.1
*     x = ----------- = --- = 0.5
*         3 - 4 * 0.7   0.2
* 
*     M = 0: lower bound A' = (2 + 0*3) / (3 + 0*4) = 2 / 3
*     N = 1: upper bound B' = (2 + 1*3) / (3 + 1*4) = 5 / 7
* </pre>
* 
* 
* Step 3: A = 2/3, B = 5/7 (a = 2, b = 3, c = 5, d = 7)
* 
* <pre>
*         3 * 0.7 - 2   0.1
*     x = ----------- = --- = 1
*         5 - 7 * 0.7   0.1
* 
*     N = 1: exact value A' = B' = (2 + 1*5) / (3 + 1*7) = 7 / 10
* </pre>
* 
* 
* which of course is obviously right.
* 
* 
* In most cases you will never get an exact integer, because of rounding 
* errors, but can stop when one of the two fractions is equal to the 
* goal to the given accuracy.
* 
* 
* [...]Just to keep you up to date, I tried out my newly invented algorithm 
* and realized it lacked one or two things. Specifically, to make it 
* work right, you have to alternate directions, first adding A + N*B and 
* then N*A + B. I tested my program for all fractions with up to three 
* digits in numerator and denominator, then started playing with the 
* problem that affects you, namely how to handle imprecision in the 
* input. I haven't yet worked out the best way to allow for error, but 
* here is my C++ function (a member function in a Fraction class 
* implemented as { short num; short denom; } ) in case you need to go to 
* this algorithm.
* 
* 
* [Edit [i_a]: tested a few stop criteria and precision settings;
* found that you can easily allow the algorithm to use the full integer
* value span: worst case iteration count was 21 - for very large prime
* numbers in the denominator and a precision set at double.Epsilon.
* Part of the code was stripped, then reinvented as I was working on a 
* proof for this system. For one, the reason to 'flip' the A/B treatment
* (i.e. the 'i&1' odd/even branch) is this: the factor N, which will
* be applied to the vector addition A + N*B is (1) an integer number to
* ensure the resulting vector (i.e. fraction) is rational, and (2) is
* determined by calculating the difference in direction between A and B.
* When the target vector direction is very close to A, the difference
* in *direction* (sort of an 'angle') is tiny, resulting in a tiny N
* value. Because the value is rounded down, A will not change. B will,
* but the number of iterations necessary to arrive at the final result
* increase significantly when the 'odd/even' processing is not included.
* Basically, odd/even processing ensures that once every second iteration
* there will be a major change in direction for any target vector M.]
* 
* 
* Edit [i_a]: further testing finds the empirical maximum
* precision to be ~ 1.0E-13, IFF you use the new high/low precision
* checks (simpler, faster) in the code (old checks have been commented out).
* Higher precision values cause the code to produce very huge fractions
* which clearly show the effect of limited floating point accuracy.
* Nevetheless, this is an impressive result.
* 
* I also changed the loop: no more odd/even processing but now we're
* looking for the biggest effect (i.e. change in direction) during EVERY
* iteration: see the new x1:x2 comparison in the code below.
* This will lead to a further reduction in the maximum number of iterations
* but I haven't checked that number now. Should be less than 21,
* I hope. ;-)
*/


double fract2dbl(const ham_fraction_t *src)
{
	ham_assert(src->denom != 0, (0));
	return src->num / (double)src->denom;
}

void to_fract_w_prec(ham_fraction_t *dst, double val, double precision)
{
	ham_fraction_t low = {0, 1};          // "A" = 0/1 (a/b)
	ham_fraction_t high = {1, 1};         // "B" = 1/1 (c/d)

	// find nearest fraction
    ham_u32_t intPart = (ham_u32_t)val;
    val -= intPart;

    for (;;)
    {
        double testLow;
        double testHigh;
        double x1;
        double x2;

        ham_assert(fract2dbl(&low) <= val, (0));
        ham_assert(fract2dbl(&high) >= val, (0));

        //         b*m - a
        //     x = -------
        //         c - d*m
        testLow = low.denom * val - low.num;
        testHigh = high.num - high.denom * val;

        // test for match:
        // 
        // m - a/b < precision
        //
        // ==>
        //
        // b * m - a < b * precision
        //
        // which is happening here: check both the current A and B fractions.
        //if (testHigh < high.denom * Precision)
        if (testHigh < precision) // [i_a] speed improvement; this is even better for irrational 'val'
        {
            break; // high is answer
        }
        //if (testLow < low.denom * Precision)
        if (testLow < precision) // [i_a] speed improvement; this is even better for irrational 'val'
        {
            // low is answer
            high = low;
            break;
        }

        x1 = testHigh / testLow;
        x2 = testLow / testHigh;

        // always choose the path where we find the largest change in direction:
        if (x1 > x2)
        {
			ham_u32_t n;
            ham_u32_t h_num;
            ham_u32_t h_denom;
            ham_u32_t l_num;
            ham_u32_t l_denom;

            //double x1 = testHigh / testLow;
            // safety checks: are we going to be out of integer bounds?
            if ((x1 + 1) * low.denom + high.denom >= (double)0xFFFFFFFF)
            {
                break;
            }

            n = (ham_u32_t)x1;    // lower bound for m
            //int m = n + 1;    // upper bound for m

            //     a + x*c
            //     ------- = m
            //     b + x*d
            h_num = n * low.num + high.num;
            h_denom = n * low.denom + high.denom;

            //ham_u32_t l_num = m * low.num + high.num;
            //ham_u32_t l_denom = m * low.denom + high.denom;
            l_num = h_num + low.num;
            l_denom = h_denom + low.denom;

            low.num = l_num;
            low.denom = l_denom;
            high.num = h_num;
            high.denom = h_denom;
        }
        else
        {
			ham_u32_t n;
            ham_u32_t h_num;
            ham_u32_t h_denom;
            ham_u32_t l_num;
            ham_u32_t l_denom;

            //double x2 = testLow / testHigh;
            // safety checks: are we going to be out of integer bounds?
            if (low.denom + (x2 + 1) * high.denom >= (double)0x7FFFFFFF)
            {
                break;
            }

            n = (ham_u32_t)x2;    // lower bound for m
            //ham_u32_t m = n + 1;    // upper bound for m

            //     a + x*c
            //     ------- = m
            //     b + x*d
            l_num = low.num + n * high.num;
            l_denom = low.denom + n * high.denom;

            //ham_u32_t h_num = low.num + m * high.num;
            //ham_u32_t h_denom = low.denom + m * high.denom;
            h_num = l_num + high.num;
            h_denom = l_denom + high.denom;

            high.num = h_num;
            high.denom = h_denom;
            low.num = l_num;
            low.denom = l_denom;
        }
        ham_assert(fract2dbl(&low) <= val, (0));
        ham_assert(fract2dbl(&high) >= val, (0));
    }

    high.num += high.denom * intPart;

    *dst = high;
}


void to_fract(ham_fraction_t *dst, double val)
{
    to_fract_w_prec(dst, val, 1.0E-13 /* float.Epsilon */ );
}

#if 0

void TestFraction(void)
{
    ham_fraction_t ret;
    double vut;

    vut = 0.1;
    to_fract(&ret, vut);
    ham_assert(fabs(vut - fract2dbl(&ret)) < 1E-9, (0));
    vut = 0.99999997;
    to_fract(&ret, vut);
    ham_assert(fabs(vut - fract2dbl(&ret)) < 1E-9, (0));
    vut = (0x40000000 - 1.0) / (0x40000000 + 1.0);
    to_fract(&ret, vut);
    ham_assert(fabs(vut - fract2dbl(&ret)) < 1E-9, (0));
    vut = 1.0 / 3.0;
    to_fract(&ret, vut);
    ham_assert(fabs(vut - fract2dbl(&ret)) < 1E-9, (0));
    vut = 1.0 / (0x40000000 - 1.0);
    to_fract(&ret, vut);
    ham_assert(fabs(vut - fract2dbl(&ret)) < 1E-9, (0));
    vut = 320.0 / 240.0;
    to_fract(&ret, vut);
    ham_assert(fabs(vut - fract2dbl(&ret)) < 1E-9, (0));
    vut = 6.0 / 7.0;
    to_fract(&ret, vut);
    ham_assert(fabs(vut - fract2dbl(&ret)) < 1E-9, (0));
    vut = 320.0 / 241.0;
    to_fract(&ret, vut);
    ham_assert(fabs(vut - fract2dbl(&ret)) < 1E-9, (0));
    vut = 720.0 / 577.0;
    to_fract(&ret, vut);
    ham_assert(fabs(vut - fract2dbl(&ret)) < 1E-9, (0));
    vut = 2971.0 / 3511.0;
    to_fract(&ret, vut);
    ham_assert(fabs(vut - fract2dbl(&ret)) < 1E-9, (0));
    vut = 3041.0 / 7639.0;
    to_fract(&ret, vut);
    ham_assert(fabs(vut - fract2dbl(&ret)) < 1E-9, (0));
    vut = 1.0 / sqrt(2);
    to_fract(&ret, vut);
    ham_assert(fabs(vut - fract2dbl(&ret)) < 1E-9, (0));
    vut = 3.1415926535897932384626433832795 /* M_PI */;
    to_fract(&ret, vut);
    ham_assert(fabs(vut - fract2dbl(&ret)) < 1E-9, (0));
}

#endif







