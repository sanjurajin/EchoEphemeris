function validateDateRange() {
    const startDate = document.getElementById('start_date').value;
    const endDate = document.getElementById('end_date').value;
    const messageElement = document.getElementById('message');

    if (startDate && endDate) {
        const start = new Date(startDate);
        const end = new Date(endDate);

        const timeDiff = Math.abs(end.getTime() - start.getTime());
        const diffDays = Math.ceil(timeDiff / (1000 * 3600 * 24));

        if (start.getMonth() === end.getMonth() && start.getFullYear() === end.getFullYear() && diffDays <= 15) {
            messageElement.innerText = 'Dates are valid!';
            return true;
        } else {
            messageElement.innerText = 'Both dates must be from the same month and year, and the difference between them should not be greater than 15 days.';
            return false;
        }
    }

    return false;
}
