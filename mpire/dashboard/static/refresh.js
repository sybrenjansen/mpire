// Make ajax calls synchronous. Note that this is deprecated because of possible user experience problems, but in our
// case this doesn't influence it. It actually makes it better
$.ajaxSetup({
    async: false
});


// Enable tooltips (uses jQuery)
$(function() {
    $(document).tooltip();
});


var progress_bar_animation_duration = 450;
var refresh_interval = 500;
var completed_pb_ids = new Set();
refresh();
setInterval(refresh, refresh_interval);


// Update progress bar given an ID and a progress (between 0-1)
function update_progress_bar(pb_id, progress)
{
    $("#pb_" + pb_id).stop().css("width", $("#pb_" + pb_id).width()).animate(
    {
        width: (progress * 100) + '%',
        easing: 'linear'
    },
    {
        duration: progress_bar_animation_duration,
        start: function(promise)
        {
            // Set text
            if (progress * $("#pb_" + pb_id + "_container").width() != 0)
            {
                $(this).text(Math.round(progress * 100) + '%');
            }
        }
    });
}


/**
 * http://stackoverflow.com/questions/2353211/hsl-to-rgb-color-conversion
 *
 * Converts an HSL color value to RGB. Conversion formula
 * adapted from http://en.wikipedia.org/wiki/HSL_color_space.
 * Assumes h, s, and l are contained in the set [0, 1] and
 * returns r, g, and b in the set [0, 255].
 *
 * @param   Number  h       The hue
 * @param   Number  s       The saturation
 * @param   Number  l       The lightness
 * @return  Array           The RGB representation
 */
function hslToRgb(h, s, l)
{
    var r, g, b;

    if(s == 0){
        r = g = b = l; // achromatic
    }else{
        function hue2rgb(p, q, t){
            if(t < 0) t += 1;
            if(t > 1) t -= 1;
            if(t < 1/6) return p + (q - p) * 6 * t;
            if(t < 1/2) return q;
            if(t < 2/3) return p + (q - p) * (2/3 - t) * 6;
            return p;
        }

        var q = l < 0.5 ? l * (1 + s) : l + s - l * s;
        var p = 2 * l - q;
        r = hue2rgb(p, q, h + 1/3);
        g = hue2rgb(p, q, h);
        b = hue2rgb(p, q, h - 1/3);
    }

    return [Math.floor(r * 255), Math.floor(g * 255), Math.floor(b * 255)];
}


// convert a number to a color using hsl
function numberToColorHsl(i)
{
    // as the function expects a value between 0 and 1, and red = 0° and green = 120°
    // we convert the input to the appropriate hue value
    var hue = i * 1.2 / 3.6;
    // we convert hsl to rgb (saturation 100%, lightness 50%)
    var rgb = hslToRgb(hue, 1, .7);
    // we format to css value and return
    return 'rgb(' + rgb[0] + ',' + rgb[1] + ',' + rgb[2] + ')';
}


// Hide part of a text if it's too long and add read more/read less functionality
function AddReadMore(tag_id, char_limit, text)
{
    // Only update when the text changes. We strip the ' ... Read more'/' ... Read less' parts (14 characters)
    var original_text = $("#" + tag_id).text();
    if (original_text.substring(0, original_text.length - 14) == text)
        return;

    if (text.length > char_limit)
    {
        var first_part = text.substring(0, char_limit);
        var second_part = text.substring(char_limit, text.length);
        var new_html = first_part + "<span id='" + tag_id + "_second_part' class='hidden'>" + second_part + "</span> " +
            "<span onclick=\"$('#" + tag_id + "_second_part').toggle(); " +
            "                $(this).text($(this).text() == '... Read more' ? '... Read less' : '... Read more');\" " +
            "      class='clickable'>... Read more</span>";
    }
    else
    {
        var new_html = text;
    }

    $("#" + tag_id).html(new_html);
}


// Refresh contents
function refresh()
{
    $.getJSON($SCRIPT_ROOT + '/_progress_bar_update', {}, function(data)
    {
        var i, worker_id, worker_prefix, task_idx, task_prefix;
        for (i = 0; i < data.result.length; i++)
        {
            var pb = data.result[i];
            var is_new = false;

            // Check if progress-bar exists
            if ($('#pb_' + pb.id).length == 0)
            {
                // If not, request new HTML for progress bar and prepend it to table
                $.getJSON($SCRIPT_ROOT + '/_progress_bar_new',
                          {pb_id: pb.id, has_insights: !$.isEmptyObject(pb.insights)}, function(new_data)
                {
                    $('#progress-table > tbody').prepend(new_data.result);
                });

                is_new = true;
            }

            // If it's already completed, do nothing, except when this is a new progress bar (e.g., when refreshed)
            if (completed_pb_ids.has(pb.id) && !is_new)
            {
                continue;
            }

            // Set new progress
            update_progress_bar(pb.id, pb.percentage);
            $('#pb_' + pb.id + '_n').text(pb.n);
            $('#pb_' + pb.id + '_total').text(pb.total);
            $('#pb_' + pb.id + '_started').text(pb.started);
            $('#pb_' + pb.id + '_duration').text(pb.duration);
            $('#pb_' + pb.id + '_remaining').text(pb.remaining);
            $('#pb_' + pb.id + '_finished').text(pb.finished);

            // Set insights, if available
            if (!$.isEmptyObject(pb.insights))
            {
                $('#pb_' + pb.id + '_insights_total_start_up_time').text(pb.insights['total_start_up_time']);
                $('#pb_' + pb.id + '_insights_start_up_time_mean').text(pb.insights['start_up_time_mean']);
                $('#pb_' + pb.id + '_insights_start_up_time_std').text(pb.insights['start_up_time_std']);
                $('#pb_' + pb.id + '_insights_start_up_ratio').text((pb.insights['start_up_ratio'] * 100.).toFixed(2))
                    .css('color', numberToColorHsl(1.0 - pb.insights['start_up_ratio']));
                $('#pb_' + pb.id + '_insights_total_init_time').text(pb.insights['total_init_time']);
                $('#pb_' + pb.id + '_insights_init_time_mean').text(pb.insights['init_time_mean']);
                $('#pb_' + pb.id + '_insights_init_time_std').text(pb.insights['init_time_std']);
                $('#pb_' + pb.id + '_insights_init_ratio').text((pb.insights['init_ratio'] * 100.).toFixed(2))
                    .css('color', numberToColorHsl(1.0 - pb.insights['waiting_ratio']));
                $('#pb_' + pb.id + '_insights_total_waiting_time').text(pb.insights['total_waiting_time']);
                $('#pb_' + pb.id + '_insights_waiting_time_mean').text(pb.insights['waiting_time_mean']);
                $('#pb_' + pb.id + '_insights_waiting_time_std').text(pb.insights['waiting_time_std']);
                $('#pb_' + pb.id + '_insights_waiting_ratio').text((pb.insights['waiting_ratio'] * 100.).toFixed(2))
                    .css('color', numberToColorHsl(1.0 - pb.insights['waiting_ratio']));
                $('#pb_' + pb.id + '_insights_total_working_time').text(pb.insights['total_working_time']);
                $('#pb_' + pb.id + '_insights_working_time_mean').text(pb.insights['working_time_mean']);
                $('#pb_' + pb.id + '_insights_working_time_std').text(pb.insights['working_time_std']);
                $('#pb_' + pb.id + '_insights_working_ratio').text((pb.insights['working_ratio'] * 100.).toFixed(2))
                    .css('color', numberToColorHsl(pb.insights['working_ratio']));
                $('#pb_' + pb.id + '_insights_total_exit_time').text(pb.insights['total_exit_time']);
                $('#pb_' + pb.id + '_insights_exit_time_mean').text(pb.insights['exit_time_mean']);
                $('#pb_' + pb.id + '_insights_exit_time_std').text(pb.insights['exit_time_std']);
                $('#pb_' + pb.id + '_insights_exit_ratio').text((pb.insights['exit_ratio'] * 100.).toFixed(2))
                    .css('color', numberToColorHsl(1.0 - pb.insights['waiting_ratio']));
                for (worker_id = 0; worker_id < pb.insights['n_completed_tasks'].length; worker_id++)
                {
                    worker_prefix = '#pb_' + pb.id + '_insights_worker_' + worker_id;
                    $(worker_prefix + '_tasks_completed').text(pb.insights['n_completed_tasks'][worker_id]);
                    $(worker_prefix + '_start_up_time').text(pb.insights['start_up_time'][worker_id]);
                    $(worker_prefix + '_init_time').text(pb.insights['init_time'][worker_id]);
                    $(worker_prefix + '_waiting_time').text(pb.insights['waiting_time'][worker_id]);
                    $(worker_prefix + '_working_time').text(pb.insights['working_time'][worker_id]);
                    $(worker_prefix + '_exit_time').text(pb.insights['exit_time'][worker_id]);
                }
                for (task_idx = 0; task_idx < pb.insights['top_5_max_task_durations'].length; task_idx++)
                {
                    task_prefix = '#pb_' + pb.id + '_insights_task_' + task_idx;
                    $(task_prefix).show();
                    $(task_prefix + '_duration').text(pb.insights['top_5_max_task_durations'][task_idx]);
                    AddReadMore("pb_" + pb.id + "_insights_task_" + task_idx + "_args", 70,
                        pb.insights['top_5_max_task_args'][task_idx]);
                }
            }

            if (pb.success)
            {
                // Success if we're at 100%
                if (pb.n == pb.total)
                {
                    $('#pb_' + pb.id).addClass('bg-success');

                    // Make lightsaber light up
                    if (!completed_pb_ids.has(pb.id))
                    {
                        $('.lightsaber').animate({color: '#00FF00'}, 300).animate({color: '#dc3545'}, 300);
                    }
                    completed_pb_ids.add(pb.id);
                }
            }
            else
            {
                // Danger if we've encountered a failure
                $('#pb_' + pb.id).addClass('bg-danger');

                // Add traceback info
                $('#pb_' + pb.id + '_traceback').show().text(pb.traceback);

                // Add a flashing flash
                $('#pb_' + pb.id + '_flash').fadeIn(200).fadeOut(200).fadeIn(200).fadeOut(200).fadeIn(200);

                // Make lightsaber light up
                if (!completed_pb_ids.has(pb.id))
                {
                    $('.lightsaber').animate({color: '#000000'}, 300).animate({color: '#dc3545'}, 300);
                }
                completed_pb_ids.add(pb.id);
            }
        }
    });
    return false;
}
