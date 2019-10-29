// Make ajax calls synchronous. Note that this is deprecated because of possible user experience problems, but in our
// case this doesn't influence it. It actually makes it better
$.ajaxSetup({
    async: false
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

// Refresh contents
function refresh()
{
    $.getJSON($SCRIPT_ROOT + '/_progress_bar_update', {}, function(data)
    {
        var i;
        for (i = 0; i < data.result.length; i++)
        {
            var pb = data.result[i];
            var is_new = false;

            // Check if progress-bar exists
            if ($('#pb_' + pb.id).length == 0)
            {
                // If not, request new HTML for progress bar and prepend it to table
                $.getJSON($SCRIPT_ROOT + '/_progress_bar_new', {pb_id: pb.id}, function(new_data)
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
                $('#pb_' + pb.id + '_traceback').show();
                $('#pb_' + pb.id + '_traceback').text(pb.traceback);

                // Add a flashing flash
                $('#pb_' + pb.id + '_flash').fadeIn(200).fadeOut(200).fadeIn(200).fadeOut(200).fadeIn(200);

                // Make lightsaber light up
                if (!completed_pb_ids.has(pb.id))
                {
                    $('.lightsaber').animate({color: '#FF0000'}, 300).animate({color: '#dc3545'}, 300);
                }
                completed_pb_ids.add(pb.id);
            }
        }
    });
    return false;
}
